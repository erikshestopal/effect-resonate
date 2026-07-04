import { Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { PaymentResult, WebhookEvent, chargeCard, sendReceipt, updateLedger, validateEvent } from "./handlers.ts";
export const repo = "example-webhook-handler-ts";
export const functionName = "processPayment";
export const sampleArgs = [
  { event_id: "evt-1", type: "payment_intent.succeeded", amount: 4999, currency: "usd", customer_id: "cus-1" },
] as const;
export const workflow = Resonate.function({ name: functionName, payload: WebhookEvent });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (event) =>
      Effect.gen(function* (): Effect.fn.Return<typeof PaymentResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run({ name: `validate-${event.event_id}`, effect: validateEvent(event) });
        const chargeId = yield* ctx
          .run({ name: `charge-${event.event_id}`, effect: chargeCard(event) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        yield* ctx.run({ name: `receipt-${event.event_id}`, effect: sendReceipt(event, chargeId) });
        return yield* ctx
          .run({ name: `ledger-${event.event_id}`, effect: updateLedger(event, chargeId) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(PaymentResult)));
      }),
  }),
);
