import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Schema } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { ResonateTest } from "effect-resonate/testing";
import { Protocol } from "effect-resonate";

const Countdown = Resonate.function("Countdown", {
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
});

const App = Resonate.group(Countdown);

const Handlers = App.toLayer(
  App.of({
    Countdown: (count, seconds) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          yield* ctx.sleep(Duration.seconds(seconds));
        }
        return "done";
      }),
  }),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.ResonateClient;
  const handle = yield* client.beginRpc(Countdown, Protocol.ExecutionId.make("countdown.1"), [3, 1]);
  return yield* handle.await;
}).pipe(Effect.provide(ResonateTest.layer(App, Handlers)));

if (import.meta.main) {
  BunRuntime.runMain(program);
}
