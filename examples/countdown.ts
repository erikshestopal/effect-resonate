import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

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

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
