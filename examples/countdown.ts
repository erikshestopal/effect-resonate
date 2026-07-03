import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import {
  Codec,
  DurablePromise,
  Protocol,
  Resonate,
  ResonateContext,
  ResonateSchedule,
  Task,
  Worker,
} from "effect-resonate";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "default");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "countdown-worker");

const countdown = Resonate.function("countdown", {
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
});

const App = Resonate.group(countdown);

const handlers = App.toLayer(
  App.of({
    countdown: (count, seconds) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          yield* ctx.run(
            Effect.sync(() => {
              const message = `Countdown: ${remaining}`;
              console.log(message);
              return message;
            }),
          );
          yield* ctx.sleep(Duration.seconds(seconds));
        }
        yield* ctx.run(
          Effect.sync(() => {
            console.log("Done!");
            return "Done!";
          }),
        );
        return "done";
      }),
  }),
);

const base = Layer.mergeAll(
  Resonate.layerHttp({ url, group, pid }).pipe(Layer.provide(BunHttpClient.layer)),
  BunCrypto.layer,
  Codec.ResonateEncryptor.layerNoop,
);

const services = Layer.mergeAll(
  Codec.ResonateCodec.layerJson,
  DurablePromise.DurablePromises.layer,
  Task.Tasks.layer,
  ResonateSchedule.Schedules.layer,
  handlers,
).pipe(Layer.provideMerge(base));

const client = Resonate.ResonateClient.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(ResonateContext.ExecutionEngine.layer.pipe(Layer.provideMerge(services))),
);

if (import.meta.main) {
  BunRuntime.runMain(
    Layer.launch(Worker.layer(App, { group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(client))),
  );
}
