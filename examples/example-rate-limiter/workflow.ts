import { Duration, Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { ApiRequest, ApiResponse, callExternalApi } from "./api.ts";
export const repo = "example-rate-limiter-ts";
export const functionName = "rateLimitedBatch";
export const sampleArgs = [
  {
    requests: [
      { id: "req-1", endpoint: "/api/v1/enrich", payload: "record-1" },
      { id: "req-2", endpoint: "/api/v1/enrich", payload: "record-2" },
    ],
    ratePerSec: 1000,
  },
] as const;
export const Payload = Schema.Struct({ requests: Schema.Array(ApiRequest), ratePerSec: Schema.Finite });
export const RateLimitResult = Schema.Struct({
  totalRequests: Schema.Finite,
  completed: Schema.Finite,
  ratePerSec: Schema.Finite,
  responses: Schema.Array(ApiResponse),
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof RateLimitResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const responses: Array<typeof ApiResponse.Type> = [];
        const interval = Duration.millis(Math.max(1, Math.floor(1000 / input.ratePerSec)));
        for (let index = 0; index < input.requests.length; index = index + 1) {
          if (index > 0) yield* ctx.sleep({ for: interval });
          const request = input.requests[index]!;
          responses.push(
            yield* ctx
              .run({ name: `api-${request.id}`, effect: callExternalApi(request, index, input.requests.length) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(ApiResponse))),
          );
        }
        return RateLimitResult.make({
          totalRequests: input.requests.length,
          completed: responses.length,
          ratePerSec: input.ratePerSec,
          responses,
        });
      }),
  }),
);
