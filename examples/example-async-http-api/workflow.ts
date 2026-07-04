import { Clock, Duration, Effect, Schema } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";

export const repo = "example-async-http-api-ts";
export const functionName = "foo";
export const sampleArgs = [{ foo: "bar" }] as const;

export const Payload = Schema.Unknown;

export const ProcessedResult = Schema.Struct({
  result: Schema.String,
  timestamp: Schema.Finite,
});

export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);

const JsonString = Schema.fromJsonString(Schema.Unknown);

export const handlers = App.toLayer(
  App.of({
    [functionName]: (data) =>
      Effect.gen(function* (): Effect.fn.Return<typeof ProcessedResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const serialized = yield* ctx.run({ effect: Schema.encodeUnknownEffect(JsonString)(data) });
        yield* ctx.sleep({ for: Duration.millis(1) });
        const timestamp = yield* ctx
          .run({ effect: Clock.currentTimeMillis })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
        return ProcessedResult.make({
          result: `Processed: ${serialized}`,
          timestamp,
        });
      }),
  }),
);
