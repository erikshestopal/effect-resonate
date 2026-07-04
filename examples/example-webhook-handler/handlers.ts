import { Duration, Effect, Schema } from "effect";

export const WebhookEvent = Schema.Struct({
  event_id: Schema.String,
  type: Schema.Literals(["payment_intent.succeeded", "payment_intent.failed"]),
  amount: Schema.Finite,
  currency: Schema.String,
  customer_id: Schema.String,
});
export type WebhookEvent = typeof WebhookEvent.Type;
export const PaymentResult = Schema.Struct({
  event_id: Schema.String,
  charge_id: Schema.String,
  status: Schema.Literal("captured"),
  amount: Schema.Finite,
  processedAt: Schema.String,
});
export const validateEvent = Effect.fn("WebhookHandler.validateEvent")(function* (event: WebhookEvent) {
  yield* Effect.logInfo(`[validate] ${event.event_id} — ${event.type}`);
  yield* Effect.sleep(Duration.millis(10));
});
export const chargeCard = Effect.fn("WebhookHandler.chargeCard")(function* (event: WebhookEvent) {
  const chargeId = `ch_${event.event_id}`;
  yield* Effect.logInfo(`[charge] ${event.customer_id} — ${event.amount} ${event.currency}`);
  yield* Effect.sleep(Duration.millis(10));
  return chargeId;
});
export const sendReceipt = Effect.fn("WebhookHandler.sendReceipt")(function* (event: WebhookEvent, chargeId: string) {
  yield* Effect.logInfo(`[receipt] ${event.customer_id} — ${chargeId}`);
});
export const updateLedger = Effect.fn("WebhookHandler.updateLedger")(function* (event: WebhookEvent, chargeId: string) {
  yield* Effect.logInfo(`[ledger] ${event.event_id} — ${chargeId}`);
  return PaymentResult.make({
    event_id: event.event_id,
    charge_id: chargeId,
    status: "captured",
    amount: event.amount,
    processedAt: "2026-01-01T00:00:00.000Z",
  });
});
