import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import { DurablePromises } from "../src/DurablePromise.ts";
import * as NetworkHttp from "../src/NetworkHttp.ts";
import * as NetworkLocal from "../src/NetworkLocal.ts";
import * as Protocol from "../src/Protocol.ts";
import { Schedules } from "../src/Schedule.ts";
import { Tasks } from "../src/Task.ts";

const serverUrl = "http://127.0.0.1:8001";

const timestamp = (millis: number) => Schema.decodeUnknownSync(Protocol.Timestamp)(millis);

const resolvedState = Schema.Literal("resolved").make("resolved");

const promiseCreateData = (id: Protocol.PromiseId) => ({
  id,
  timeoutAt: timestamp(Date.now() + 60_000),
  param: Protocol.emptyValue,
  tags: Protocol.emptyTags,
});

const promiseSettleData = (id: Protocol.PromiseId) => ({
  id,
  state: resolvedState,
  value: Protocol.emptyValue,
});

const localLayer = Layer.mergeAll(
  NetworkLocal.layer({ tickInterval: Duration.hours(24), retryTimeout: Duration.seconds(5) }),
  BunCrypto.layer,
);

const liveBaseLayer = (pid: Protocol.ProcessId) =>
  Layer.mergeAll(
    NetworkHttp.layer({ url: serverUrl, group: "differential", pid }).pipe(Layer.provide(BunHttpClient.layer)),
    BunCrypto.layer,
  );

const localClientLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer, Schedules.layer).pipe(
  Layer.provide(localLayer),
);

const localRunLayer = Layer.mergeAll(localLayer, localClientLayer);

const liveRunLayer = (pid: Protocol.ProcessId) => {
  const base = liveBaseLayer(pid);
  const clients = Layer.mergeAll(DurablePromises.layer, Tasks.layer, Schedules.layer).pipe(Layer.provide(base));
  return Layer.mergeAll(base, clients);
};

const scenario = Effect.fn("Differential.scenario")(function* (prefix: string) {
  const promises = yield* DurablePromises;
  const tasks = yield* Tasks;
  const schedules = yield* Schedules;
  const id = Protocol.PromiseId.make(`${prefix}-promise`);
  const taskId = Protocol.TaskId.make(`${prefix}-task`);
  const scheduleId = Protocol.ScheduleId.make(`${prefix}-schedule`);
  const pid = Protocol.ProcessId.make(`${prefix}-pid`);
  const ttl = Duration.millis(250);

  const created = yield* promises.create(promiseCreateData(id));
  const got = yield* promises.get(id);
  const settled = yield* promises.settle(promiseSettleData(id));
  const task = yield* tasks.create({
    pid,
    ttl,
    action: Protocol.PromiseCreateRequest.make({
      head: Protocol.RequestHead.make({
        corrId: Protocol.CorrelationId.make(`${prefix}-task-corr`),
        version: Protocol.protocolVersion,
      }),
      data: promiseCreateData(taskId),
    }),
  });
  const schedule = yield* schedules.create({
    id: scheduleId,
    cron: "* * * * *",
    promiseId: `${prefix}-scheduled-{{.timestamp}}`,
    promiseTimeout: Duration.seconds(30),
    promiseParam: Protocol.emptyValue,
    promiseTags: Protocol.emptyTags,
  });
  yield* schedules.delete(scheduleId);

  return {
    promise: { created: created.state, got: got.state, settled: settled.state },
    task: { promise: task.promise.state, task: task.task?.state, preload: task.preload.length },
    schedule: { id: schedule.id, cron: schedule.cron },
  };
});

const withResonateDev = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  Effect.acquireUseRelease(
    Effect.sync(() => Bun.spawn(["resonate", "dev"], { stdout: "pipe", stderr: "pipe" })),
    () => Effect.sleep(Duration.seconds(2)).pipe(Effect.andThen(effect)),
    (process) => Effect.sync(() => process.kill()),
  );

describe("differential harness", () => {
  it.effect("compares local oracle with shipped server when resonate CLI is installed", () =>
    Effect.gen(function* () {
      const resonate = Bun.which("resonate");
      if (resonate === null) {
        console.warn("[DIFFERENTIAL SKIPPED] resonate CLI not found; install it to run shipped-server parity.");
        expect(resonate).toBeNull();
        return;
      }

      const prefix = `diff-${Date.now()}`;
      const local = yield* scenario(prefix).pipe(Effect.provide(localRunLayer));
      const live = yield* withResonateDev(
        scenario(prefix).pipe(Effect.provide(liveRunLayer(Protocol.ProcessId.make(`${prefix}-live`)))),
      );
      expect(live).toEqual(local);
    }),
  );
});
