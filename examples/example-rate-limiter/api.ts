import { Duration, Effect, Schema } from "effect";
export const ApiRequest = Schema.Struct({ id: Schema.String, endpoint: Schema.String, payload: Schema.String });
export type ApiRequest = typeof ApiRequest.Type;
export const ApiResponse = Schema.Struct({
  requestId: Schema.String,
  endpoint: Schema.String,
  status: Schema.Literal("ok"),
  data: Schema.String,
  latencyMs: Schema.Finite,
});
export const callExternalApi = Effect.fn("RateLimiter.callExternalApi")(function* (
  request: ApiRequest,
  index: number,
  total: number,
) {
  yield* Effect.logInfo(`[${index + 1}/${total}] ${request.id} ${request.endpoint}`);
  yield* Effect.sleep(Duration.millis(10));
  return ApiResponse.make({
    requestId: request.id,
    endpoint: request.endpoint,
    status: "ok",
    data: `response for ${request.payload}`,
    latencyMs: 10,
  });
});
