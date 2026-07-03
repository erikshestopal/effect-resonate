import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Layer, Option, Schema, SchemaParser } from "effect";
import { TestClock } from "effect/testing";
import { ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as NetworkLocal from "../src/NetworkLocal.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import { Tasks } from "../src/Task.ts";
import * as Worker from "../src/Worker.ts";

const Workflow = Resonate.function("WorkerWorkflow", {
  payload: Schema.Number,
});

const Blocking = Resonate.function("BlockingWorkflow", {
  payload: Schema.Number,
});

const Suspend = Resonate.function("SuspendWorkflow", {
  payload: Schema.Number,
});

const Sleep = Resonate.function("SleepWorkflow", {
  payload: Schema.Number,
});

const group = Resonate.group(Workflow, Blocking, Suspend, Sleep);
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
const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer);
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
        const stepped = yield* ctx.run(Effect.succeed(value + 1));
        return Number(stepped) + 1;
      }),
    BlockingWorkflow: () => Effect.never,
    SuspendWorkflow: () =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const child = yield* ctx.beginRun(Effect.die("external promise should not execute locally"), {
          id: Protocol.PromiseId.make("worker-suspend-1.0"),
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
  }),
);
const workerLayer = Worker.layer(group, {
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("worker-1"),
  ttl: Duration.seconds(30),
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
      const codec = yield* ResonateCodec;
      const handle = yield* client.beginRpc(Workflow, Protocol.ExecutionId.make("worker-root-1"), [1]);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const promise = yield* promises.get(handle.id);
      expect(promise.state).toBe("resolved");
      expect(yield* codec.decode(promise.value)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("heartbeats the actual held task/version so leases stay acquired", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRpc(Blocking, Protocol.ExecutionId.make("worker-heartbeat-1"), [0]);
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
      const codec = yield* ResonateCodec;
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
      const codec = yield* ResonateCodec;
      const handle = yield* client.beginRpc(Sleep, Protocol.ExecutionId.make("worker-sleep-1"), [1]);

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
      const handle = yield* client.beginRpc(Sleep, Protocol.ExecutionId.make("worker-sleep-clamp-1"), [2], {
        timeout: Duration.minutes(30),
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
});
