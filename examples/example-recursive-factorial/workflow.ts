import { Config, Effect, Schema } from "effect";
import { Protocol, Resonate, ResonateContext } from "effect-resonate";

export const repo = "example-recursive-factorial-ts";
export const functionName = "factorial";
export const sampleArgs = [4] as const;

export const FactorialInput = Schema.Finite;
export const factorial = Resonate.function({ name: functionName, payload: FactorialInput });
export const App = Resonate.group(factorial);

const workerGroup = Effect.gen(function* () {
  const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-recursive-factorial-ts"));
  return Protocol.WorkerGroup.make(groupName);
});

export const handlers = App.toLayer(
  App.of({
    factorial: (n) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run({ name: `log-factorial-${n}`, effect: Effect.logInfo(`Calculating factorial(${n})`) });
        if (n <= 1) {
          return 1;
        }
        const result = yield* ctx
          .rpc({ target: factorial, args: [n - 1], options: { target: yield* workerGroup } })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        return n * result;
      }),
  }),
);
