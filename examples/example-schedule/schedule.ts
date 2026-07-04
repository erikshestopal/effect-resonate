import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer } from "effect";
import { NetworkHttp, Protocol, Resonate, ResonateSchedule } from "effect-resonate";
import { workflow } from "./workflow.ts";

export const reportSchedule = (group: Protocol.WorkerGroup) =>
  Resonate.schedule({
    id: Protocol.ScheduleId.make("daily_report"),
    cron: "* * * * *",
    function: workflow,
    payload: [123],
    target: group,
    timeout: Duration.hours(24),
  });

export const createSchedule = Effect.gen(function* () {
  const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("workers"));
  const schedule = yield* reportSchedule(Protocol.WorkerGroup.make(groupName)).create;
  yield* Effect.logInfo("Schedule created. Start the worker to process executions.", schedule);
  return schedule;
});

export const createScheduleLayer = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    return Layer.effectDiscard(createSchedule).pipe(
      Layer.provide(ResonateSchedule.Schedules.layer),
      Layer.provide(NetworkHttp.layer({ url })),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(createScheduleLayer));
}
