import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-rate-limiter-ts";
export const functionName = "rateLimitedBatch";
export const sampleArgs = [
  { requests: [{ id: "req-1", endpoint: "/v1/orders", payload: "{}" }], ratePerSec: 1000 },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-rate-limiter-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-rate-limiter-ts-worker");

const Payload = Schema.Struct({
  requests: Schema.Array(Schema.Struct({ id: Schema.String, endpoint: Schema.String, payload: Schema.String })),
  ratePerSec: Schema.Number,
});
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(
            Effect.logInfo(`rate limited ${input.requests.length} requests`).pipe(
              Effect.as(`rate limited ${input.requests.length} requests`),
            ),
          ),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
