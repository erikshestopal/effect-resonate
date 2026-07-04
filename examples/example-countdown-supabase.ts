import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

export const repo = "example-countdown-supabase-ts";
export const functionName = "countdown";
export const sampleArgs = [5, 0, "mock"] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-countdown-supabase-ts --func countdown --json-args '[5,0,"mock"]' example-countdown-supabase-ts-demo

const Payload = Schema.Tuple([Schema.Finite, Schema.Finite, Schema.String]);
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (count, delay, notificationUrl) =>
      Effect.gen(function* () {
        const ctx = yield* Resonate.Context;
        for (let i = count; i > 0; i -= 1) {
          yield* ctx.run({ effect: Effect.logInfo(`Countdown: ${i} -> ${notificationUrl}`) });
          yield* ctx.sleep({ for: Duration.seconds(delay) });
        }
        yield* ctx.run({ effect: Effect.logInfo(`Done -> ${notificationUrl}`) });
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
