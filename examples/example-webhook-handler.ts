import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-webhook-handler-ts";
export const functionName = "processPayment";
export const sampleArgs = [
  { event_id: "evt-1", type: "payment_intent.succeeded", amount: 42, currency: "USD", customer_id: "cus-1" },
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-webhook-handler-ts --func processPayment --json-args '[{"event_id":"evt-1","type":"payment_intent.succeeded","amount":42,"currency":"USD","customer_id":"cus-1"}]' example-webhook-handler-ts-demo

const Payload = Schema.Struct({
  event_id: Schema.String,
  type: Schema.String,
  amount: Schema.Finite,
  currency: Schema.String,
  customer_id: Schema.String,
});
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`validated ${input.event_id}`).pipe(Effect.as(`validated ${input.event_id}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`charged ${input.amount} ${input.currency}`).pipe(
              Effect.as(`charged ${input.amount} ${input.currency}`),
            ),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`ledger ${input.event_id}`).pipe(Effect.as(`ledger ${input.event_id}`)),
          }),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-webhook-handler-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-webhook-handler-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
