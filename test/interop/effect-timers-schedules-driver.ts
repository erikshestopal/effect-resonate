import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { DateTime, Duration, Effect, Layer, Schema } from "effect";
import { currentCodec } from "../../src/Codec.ts";
import * as NetworkHttp from "../../src/network/Http.ts";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import { Schedules } from "../../src/Schedule.ts";
import * as Worker from "../../src/Worker.ts";

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(Bun.env.RESONATE_GROUP ?? "default");
const pid = Protocol.ProcessId.make(Bun.env.RESONATE_PID ?? "timers-schedules-worker");
const request = async (kind: string, data: object = {}) =>
  (
    await (
      await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kind, head: { corrId: `${kind}-${crypto.randomUUID()}`, version: "2026-04-01" }, data }),
      })
    ).json()
  ).data;
const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));
const waitForPromiseState = async (id: string, state: string) => {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const snap = await request("debug.snap");
    if (
      snap.promises.some(
        (promise: { readonly id: string; readonly state: string }) => promise.id === id && promise.state === state,
      )
    ) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`${id} did not reach ${state}`);
};

const SleepFor = Resonate.function({ name: "sleepFor", payload: Schema.Number });
const SleepUntil = Resonate.function({ name: "sleepUntil", payload: Schema.Number });
const ChildSleeper = Resonate.function({ name: "childSleeper", payload: Schema.Number });
const ExternalTimeout = Resonate.function({ name: "externalTimeout", payload: Schema.Number });
const ScheduledEcho = Resonate.function({ name: "scheduledEcho", payload: Schema.String });
const External = Resonate.promise({ name: "timerExternal", success: Schema.Unknown, error: Schema.Unknown });
const App = Resonate.group(SleepFor, SleepUntil, ChildSleeper, ExternalTimeout, ScheduledEcho);
const handlers = App.toLayer(
  App.of({
    sleepFor: (millis) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        yield* ctx.sleep({ for: Duration.millis(millis) });
        return { slept: millis };
      }),
    sleepUntil: (time) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        yield* ctx.sleep({ until: DateTime.makeUnsafe(time) });
        return { until: time };
      }),
    childSleeper: (millis) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        return yield* ctx.rpc({ target: SleepFor, args: [millis] });
      }),
    externalTimeout: (millis) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const p = yield* ctx.promise({
          declaration: External,
          options: { id: Protocol.PromiseId.make(`${ctx.info.id}.0`), timeout: Duration.millis(millis) },
        });
        return yield* p.await;
      }),
    scheduledEcho: (input) => Effect.succeed({ scheduled: input }),
  }),
);
const networkLayer = NetworkHttp.layer({ url, group, pid }).pipe(Layer.provideMerge(BunHttpClient.layer));
const clientLayer = Resonate.Client.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);
const scheduleLayer = Schedules.layer.pipe(Layer.provideMerge(networkLayer), Layer.provideMerge(BunCrypto.layer));
const worker = Worker.layer({ group: App, worker: { group, pid, ttl: Duration.seconds(5) } }).pipe(
  Layer.provideMerge(handlers),
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const codec = yield* currentCodec;
  const now = 1_783_141_580_000;
  const observed: Record<string, unknown> = {};
  yield* client.beginRpc({
    targetFunction: SleepFor,
    executionId: Protocol.ExecutionId.make("timer-root-timeout"),
    args: [60_000],
    options: { target: group, timeout: Duration.millis(500) },
  });
  yield* client.beginRpc({
    targetFunction: ChildSleeper,
    executionId: Protocol.ExecutionId.make("timer-child-timeout"),
    args: [60_000],
    options: { target: group, timeout: Duration.millis(500) },
  });
  yield* client.beginRpc({
    targetFunction: ExternalTimeout,
    executionId: Protocol.ExecutionId.make("timer-external-timeout"),
    args: [500],
    options: { target: group, timeout: Duration.seconds(5) },
  });
  yield* client.beginRpc({
    targetFunction: SleepFor,
    executionId: Protocol.ExecutionId.make("timer-sleep-for"),
    args: [1_000],
    options: { target: group },
  });
  yield* client.beginRpc({
    targetFunction: SleepUntil,
    executionId: Protocol.ExecutionId.make("timer-sleep-until"),
    args: [now + 1_000],
    options: { target: group },
  });
  yield* Effect.promise(() => sleep(500));
  observed.tick = yield* Effect.promise(() => request("debug.tick", { time: now + 2_000 }));
  yield* Effect.promise(() => sleep(500));
  const schedule = Resonate.schedule({
    id: Protocol.ScheduleId.make("timer-schedule"),
    cron: "* * * * *",
    function: ScheduledEcho,
    payload: ["tick"],
    target: group,
  });
  const scheduleRecord = yield* schedule.create;
  observed.scheduleCreated = Schema.encodeSync(Protocol.ScheduleRecord)(scheduleRecord);
  observed.scheduleParam = yield* codec.decode(scheduleRecord.promiseParam);
  observed.scheduleTick = yield* Effect.promise(() => request("debug.tick", { time: now + 65_000 }));
  yield* Effect.promise(() => sleep(500));
  yield* schedule.delete;
  observed.scheduleDeleted = true;
  observed.afterDeleteTick = yield* Effect.promise(() => request("debug.tick", { time: now + 125_000 }));
  yield* Effect.promise(() => waitForPromiseState("timer-sleep-for", "resolved"));
  return observed;
});
const result = await Effect.runPromise(
  program.pipe(Effect.provide(Layer.mergeAll(clientLayer, scheduleLayer, worker))),
);
console.log(JSON.stringify(result));
