import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

const countdown = Resonate.function({ name: "countdown", payload: Schema.Tuple([Schema.Finite, Schema.Finite]) });

// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@default --func countdown --json-args '[3,1]' countdown-demo
const App = Resonate.group(countdown);

const handlers = App.toLayer(
  App.of({
    countdown: (count, seconds) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          const message = `Countdown: ${remaining}`;
          yield* ctx.run({ effect: Effect.logInfo(message).pipe(Effect.as(message)) });
          yield* ctx.sleep({ for: Duration.seconds(seconds) });
        }
        yield* ctx.run({ effect: Effect.logInfo("Done!").pipe(Effect.as("Done!")) });
        return "done";
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("default"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("countdown-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
