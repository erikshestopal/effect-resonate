import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-webhook-handler-ts";
export const functionName = "processPayment";
export const sampleArgs = [
  { event_id: "evt-1", type: "payment_intent.succeeded", amount: 42, currency: "USD", customer_id: "cus-1" },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-webhook-handler-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-webhook-handler-ts-worker");

const Payload = Schema.Struct({
  event_id: Schema.String,
  type: Schema.String,
  amount: Schema.Number,
  currency: Schema.String,
  customer_id: Schema.String,
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
          yield* ctx.run(Effect.logInfo(`validated ${input.event_id}`).pipe(Effect.as(`validated ${input.event_id}`))),
        );
        results.push(
          yield* ctx.run(
            Effect.logInfo(`charged ${input.amount} ${input.currency}`).pipe(
              Effect.as(`charged ${input.amount} ${input.currency}`),
            ),
          ),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`ledger ${input.event_id}`).pipe(Effect.as(`ledger ${input.event_id}`))),
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
