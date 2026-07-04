import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Layer, Option, Schema, SchemaParser } from "effect";
import { TestClock } from "effect/testing";
import { currentCodec, ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { ResonateNetwork } from "../src/network/network.ts";
import { makeRequestHead } from "./support/testing.ts";
import * as NetworkLocal from "../src/network/local.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import { Schedules } from "../src/Schedule.ts";
import { Tasks } from "../src/Task.ts";
import * as Worker from "../src/Worker.ts";

const Workflow = Resonate.function({ name: "WorkerWorkflow", payload: Schema.Number });

const Blocking = Resonate.function({ name: "BlockingWorkflow", payload: Schema.Number });

const Suspend = Resonate.function({ name: "SuspendWorkflow", payload: Schema.Number });

const Sleep = Resonate.function({ name: "SleepWorkflow", payload: Schema.Number });

const RemoteRoot = Resonate.function({ name: "WorkerRemoteRoot", payload: Schema.Number });

const RemoteChild = Resonate.function({ name: "WorkerRemoteChild", payload: Schema.Number });

const Scheduled = Resonate.function({ name: "ScheduledWorkflow", payload: Schema.Struct({ id: Schema.String }) });

const group = Resonate.group(Workflow, Blocking, Suspend, Sleep, RemoteRoot, RemoteChild, Scheduled);
const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({
    group: "workers",
    pid: "worker-1",
    tickInterval: Duration.hours(24),
    retryTimeout: Duration.seconds(5),
  }),
  BunCrypto.layer,
  ResonateEncryptor.layerNoop,
);
const codecLayer = ResonateCodec.layerJson;
const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer, Schedules.layer);
const clientLayer = Resonate.ResonateClient.layer({
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("client-1"),
  ttl: Duration.seconds(30),
});
const engineLayer = ExecutionEngine.layer;
const handlerLayer = group.toLayer(
  group.of({
    WorkerWorkflow: (value) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const stepped = yield* ctx.run({ effect: Effect.succeed(value + 1) });
        return Number(stepped) + 1;
      }),
    BlockingWorkflow: () => Effect.never,
    SuspendWorkflow: () =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const child = yield* ctx.beginRun({
          effect: Effect.die("external promise should not execute locally"),
          options: {
            id: Protocol.PromiseId.make("worker-suspend-1.0"),
          },
        });
        yield* child.await;
        return "done";
      }),
    SleepWorkflow: (hours) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        yield* ctx.sleep(Duration.hours(Number(hours)));
        return "awake";
      }),
    WorkerRemoteRoot: (value) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return yield* ctx.rpc({
          target: RemoteChild,
          args: [Number(value) + 1],
          options: { target: Protocol.WorkerGroup.make("workers") },
        });
      }),
    WorkerRemoteChild: (value) => Effect.succeed(Number(value) + 1),
    ScheduledWorkflow: (payload) => Effect.succeed(`scheduled:${payload.id}`),
  }),
);
const workerLayer = Worker.layer({
  group: group,
  worker: {
    group: Protocol.WorkerGroup.make("workers"),
    pid: Protocol.ProcessId.make("worker-1"),
    ttl: Duration.seconds(30),
  },
});
const coreServicesLayer = Layer.mergeAll(codecLayer, protocolLayer, handlerLayer).pipe(Layer.provideMerge(baseLayer));
const engineServicesLayer = engineLayer.pipe(Layer.provideMerge(coreServicesLayer));
const clientServicesLayer = clientLayer.pipe(Layer.provideMerge(engineServicesLayer));
const layer = workerLayer.pipe(Layer.provideMerge(clientServicesLayer));

const snap = Effect.fn("WorkerTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  return response.data;
});

const tick = Effect.fn("WorkerTest.tick")(function* (millis: number) {
  const network = yield* ResonateNetwork;
  yield* network.send(
    Protocol.DebugTickRequest.make({
      head: yield* makeRequestHead,
      data: { time: Schema.decodeUnknownSync(Protocol.Timestamp)(millis) },
    }),
  );
});

const timeoutAt = Schema.decodeUnknownSync(Protocol.Timestamp)(86_400_000);

const workerTarget = Schema.decodeUnknownSync(Protocol.TargetAddressFromString)("poll://any@workers");
const externalTarget = Schema.decodeUnknownSync(Protocol.TargetAddressFromString)("poll://any@external");

const rootTags = (id: Protocol.PromiseId): Protocol.Tags =>
  Protocol.Tags.make({
    reserved: {
      "resonate:target": workerTarget,
      "resonate:origin": id,
      "resonate:prefix": id,
      "resonate:branch": id,
      "resonate:parent": id,
      "resonate:scope": Schema.Literal("global").make("global"),
    },
    unrecognized: {},
    user: {},
  });

const externalTags = (root: Protocol.PromiseId): Protocol.Tags =>
  Protocol.Tags.make({
    reserved: {
      "resonate:target": externalTarget,
      "resonate:origin": root,
      "resonate:prefix": root,
      "resonate:branch": root,
      "resonate:parent": root,
      "resonate:scope": Schema.Literal("global").make("global"),
    },
    unrecognized: {},
    user: {},
  });

describe("Worker", () => {
  it.effect("acquires execute messages and fulfills client RPCs", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handle = yield* client.beginRpc({
        targetFunction: Workflow,
        executionId: Protocol.ExecutionId.make("worker-root-1"),
        args: [1],
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const promise = yield* promises.get(handle.id);
      expect(promise.state).toBe("resolved");
      expect(yield* codec.decode(promise.value)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("suspends and resumes a targeted context RPC through the worker loop", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handle = yield* client.beginRpc({
        targetFunction: RemoteRoot,
        executionId: Protocol.ExecutionId.make("worker-remote-1"),
        args: [1],
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const promise = yield* promises.get(handle.id);
      const child = yield* promises.get(Protocol.PromiseId.make("worker-remote-1.0"));
      expect(promise.state).toBe("resolved");
      expect(child.tags.reserved["resonate:target"]?.address).toBe("local://any@workers");
      expect(yield* codec.decode(promise.value)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("heartbeats the actual held task/version so leases stay acquired", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRpc({
        targetFunction: Blocking,
        executionId: Protocol.ExecutionId.make("worker-heartbeat-1"),
        args: [0],
      });
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(20));
      yield* tick(31_000);

      const state = yield* snap();
      const task = state.tasks.find((task) => task.id === handle.id);
      expect(task?.state).toBe("acquired");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("suspends on a pending external await and resumes by replay after settlement", () =>
    Effect.gen(function* () {
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const root = Protocol.PromiseId.make("worker-suspend-1");
      const child = Protocol.PromiseId.make("worker-suspend-1.0");

      yield* promises.create({
        id: child,
        timeoutAt,
        param: Protocol.emptyValue,
        tags: externalTags(root),
      });
      yield* promises.create({
        id: root,
        timeoutAt,
        param: yield* codec.encode({ func: Suspend.name, args: [0], version: Suspend.version }),
        tags: rootTags(root),
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const suspended = yield* snap();
      expect(suspended.tasks.find((task) => task.id === root)?.state).toBe("suspended");
      expect(suspended.callbacks).toEqual([{ awaiter: root, awaited: child }]);

      yield* promises.settle({
        id: child,
        state: Schema.Literal("resolved").make("resolved"),
        value: yield* codec.encode("ready"),
      });
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const resumed = yield* promises.get(root);
      expect(resumed.state).toBe("resolved");
      expect(yield* codec.decode(resumed.value)).toBe("done");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("suspends on durable sleep and resumes when the timer resolves", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const codec = yield* currentCodec;
      const handle = yield* client.beginRpc({
        targetFunction: Sleep,
        executionId: Protocol.ExecutionId.make("worker-sleep-1"),
        args: [1],
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const suspended = yield* snap();
      const timer = suspended.promises.find((promise) => promise.id === "worker-sleep-1.0");
      expect(suspended.tasks.find((task) => task.id === handle.id)?.state).toBe("suspended");
      expect(timer?.state).toBe("pending");
      expect(timer?.tags.reserved["resonate:timer"]).toBe("true");
      expect(timer?.tags.reserved["resonate:target"]).toBeUndefined();
      expect(timer?.tags.reserved["resonate:scope"]).toBe("global");
      expect(timer?.tags.reserved["resonate:branch"]).toBe("worker-sleep-1.0");
      expect(timer?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(DateTime.toEpochMillis(timer?.timeoutAt ?? DateTime.makeUnsafe(-1))).toBe(3_600_000);

      yield* TestClock.adjust(Duration.hours(1));
      yield* tick(3_600_000);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const resolvedTimer = yield* promises.get(Protocol.PromiseId.make("worker-sleep-1.0"));
      expect(resolvedTimer.state).toBe("resolved");
      if (resolvedTimer.state === "pending") {
        return yield* Effect.die("timer was not resolved");
      }
      expect(Option.map(resolvedTimer.settledAt, DateTime.toEpochMillis)).toEqual(Option.some(3_600_000));

      const resumed = yield* promises.get(handle.id);
      expect(resumed.state).toBe("resolved");
      expect(yield* codec.decode(resumed.value)).toBe("awake");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("clamps durable sleep to the parent timeout", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const handle = yield* client.beginRpc({
        targetFunction: Sleep,
        executionId: Protocol.ExecutionId.make("worker-sleep-clamp-1"),
        args: [2],
        options: { timeout: Duration.minutes(30) },
      });

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const suspended = yield* snap();
      const timer = suspended.promises.find((promise) => promise.id === "worker-sleep-clamp-1.0");
      expect(DateTime.toEpochMillis(timer?.timeoutAt ?? DateTime.makeUnsafe(-1))).toBe(1_800_000);

      yield* TestClock.adjust(Duration.minutes(30));
      yield* tick(1_800_000);

      const resolvedTimer = yield* promises.get(Protocol.PromiseId.make("worker-sleep-clamp-1.0"));
      const timedOutRoot = yield* promises.get(handle.id);
      expect(resolvedTimer.state).toBe("resolved");
      expect(timedOutRoot.state).toBe("rejected_timedout");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("creates schedules through Resonate.schedule.layer and executes fired promises", () => {
    const scheduled = Resonate.schedule({
      id: Protocol.ScheduleId.make("api-nightly"),
      cron: "* * * * *",
      function: Scheduled,
      payload: [{ id: "order-1" }],
      timeout: Duration.seconds(30),
      target: Protocol.WorkerGroup.make("workers"),
    });

    return Effect.gen(function* () {
      const codec = yield* currentCodec;
      const promises = yield* DurablePromises;

      const created = yield* scheduled.get;
      expect(created.id).toBe("api-nightly");
      expect(created.cron).toBe("* * * * *");
      expect(created.promiseId).toBe("{{.id}}.{{.timestamp}}");
      expect(created.promiseTags.reserved["resonate:target"]?.address).toBe("local://any@workers");
      expect(yield* codec.decode(created.promiseParam)).toEqual({
        func: "ScheduledWorkflow",
        args: [{ id: "order-1" }],
        version: 1,
      });

      yield* TestClock.adjust(Duration.minutes(1));
      yield* tick(60_000);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const fired = yield* promises.get(Protocol.PromiseId.make("api-nightly.60000"));
      expect(fired.state).toBe("resolved");
      if (fired.state === "pending") {
        return yield* Effect.die("scheduled promise was not resolved");
      }
      expect(yield* codec.decode(fired.value)).toBe("scheduled:order-1");
    }).pipe(Effect.provide(scheduled.layer), Effect.provide(layer));
  });

  it.effect("keeps schedule create idempotent without drift checks", () =>
    Effect.gen(function* () {
      const schedules = yield* Schedules;
      const initial = Resonate.schedule({
        id: Protocol.ScheduleId.make("api-idempotent"),
        cron: "* * * * *",
        function: Scheduled,
        payload: [{ id: "first" }],
        target: Protocol.WorkerGroup.make("workers"),
      });
      const changed = Resonate.schedule({
        id: Protocol.ScheduleId.make("api-idempotent"),
        cron: "*/5 * * * *",
        function: Scheduled,
        payload: [{ id: "changed" }],
        target: Protocol.WorkerGroup.make("workers"),
      });

      const created = yield* initial.create;
      const recreated = yield* changed.create;
      const stored = yield* schedules.get(Protocol.ScheduleId.make("api-idempotent"));

      expect(recreated.cron).toBe(created.cron);
      expect(recreated.promiseParam).toEqual(created.promiseParam);
      expect(stored).toEqual(created);

      yield* schedules.delete(Protocol.ScheduleId.make("api-idempotent"));
      yield* TestClock.adjust(Duration.minutes(1));
      yield* tick(60_000);
      expect((yield* snap()).promises.some((promise) => promise.id === "api-idempotent.60000")).toBe(false);
    }).pipe(Effect.provide(layer)),
  );
});
