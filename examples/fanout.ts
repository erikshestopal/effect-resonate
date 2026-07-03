import { BunRuntime } from "@effect/platform-bun";
import { Effect, Schema } from "effect";
import { Protocol, Resonate, ResonateContext } from "effect-resonate";
import { ResonateTest } from "effect-resonate/testing";

const Fanout = Resonate.function("Fanout", {
  payload: Schema.Number,
});

const App = Resonate.group(Fanout);

const Handlers = App.toLayer(
  App.of({
    Fanout: (value) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const left = yield* ctx.beginRun(Effect.succeed(Number(value) + 1));
        const right = yield* ctx.beginRun(Effect.succeed(Number(value) + 2));
        const results = yield* ctx.all([left.await, right.await]);
        return Number(results[0]) + Number(results[1]);
      }),
  }),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.ResonateClient;
  const handle = yield* client.beginRpc(Fanout, Protocol.ExecutionId.make("fanout.1"), [1]);
  return yield* handle.await;
}).pipe(Effect.provide(ResonateTest.layer(App, Handlers)));

if (import.meta.main) {
  BunRuntime.runMain(program);
}
