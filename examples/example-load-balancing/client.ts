import { Effect, Schema } from "effect";
export const ComputeRequest = Schema.Struct({ id: Schema.String, computeCost: Schema.Finite });
export type ComputeRequest = typeof ComputeRequest.Type;
export const makeComputeRequest = (id: string, computeCost: number) => ComputeRequest.make({ id, computeCost });
export const beginCompute = Effect.fn("LoadBalancing.beginCompute")(function* (request: ComputeRequest) {
  yield* Effect.logInfo(`[client] dispatch ${request.id} to poll://any@workers`);
  return request;
});
