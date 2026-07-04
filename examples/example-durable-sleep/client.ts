import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer } from "effect";
import { Client, NetworkHttp, Protocol } from "effect-resonate";
import { workflow } from "./workflow.ts";

export const runSleepClient = Effect.gen(function* () {
  const client = yield* Client.ResonateClient;
  const result = yield* client.rpc({
    executionId: Protocol.ExecutionId.make("sleep-workflow-2"),
    targetFunction: workflow,
    args: [5000],
    options: {
      target: Protocol.WorkerGroup.make("workers"),
      timeout: Duration.minutes(1),
    },
  });
  yield* Effect.logInfo("Durable sleep result", result);
  return result;
});

export const clientLayer = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("client"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-durable-sleep-client"));
    return Layer.effectDiscard(runSleepClient).pipe(
      Layer.provide(
        Client.ResonateClient.layer({
          group: Protocol.WorkerGroup.make(groupName),
          pid: Protocol.ProcessId.make(pidName),
        }),
      ),
      Layer.provide(NetworkHttp.layer({ url })),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(clientLayer));
}
