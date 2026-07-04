import { Duration, Effect, Schema } from "effect";

export const OrderItem = Schema.Struct({ sku: Schema.String, qty: Schema.Finite, price: Schema.Finite });
export const Order = Schema.Struct({
  id: Schema.String,
  items: Schema.Array(OrderItem),
  customer: Schema.Struct({ id: Schema.String, email: Schema.String }),
  simulateCrash: Schema.optional(Schema.Boolean),
});
export type Order = typeof Order.Type;

export const OrderResult = Schema.Struct({
  orderId: Schema.String,
  inventoryReserved: Schema.Boolean,
  paymentCharged: Schema.Boolean,
  confirmationSent: Schema.Boolean,
  total: Schema.Finite,
  chargeId: Schema.String,
});

export const validateOrder = Effect.fn("ExpressIntegration.validateOrder")(function* (order: Order) {
  yield* Effect.logInfo(`[validate] order ${order.id} — ${order.items.length} item(s)`);
  yield* Effect.sleep(Duration.millis(10));
  return true;
});

export const reserveInventory = Effect.fn("ExpressIntegration.reserveInventory")(function* (order: Order) {
  yield* Effect.logInfo(`[inventory] order ${order.id} — reserved ${order.items.length} item(s)`);
  yield* Effect.sleep(Duration.millis(10));
  return true;
});

export const chargePayment = Effect.fn("ExpressIntegration.chargePayment")(function* (order: Order) {
  const total = order.items.reduce((sum, item) => sum + item.price * item.qty, 0);
  const chargeId = `ch_${order.id}`;
  yield* Effect.logInfo(`[payment] order ${order.id} — charged ${total}`);
  return chargeId;
});

export const sendConfirmation = Effect.fn("ExpressIntegration.sendConfirmation")(function* (
  order: Order,
  chargeId: string,
) {
  yield* Effect.logInfo(`[email] order ${order.id} — sent to ${order.customer.email} (${chargeId})`);
  return true;
});

export const orderTotal = (order: Order) => order.items.reduce((sum, item) => sum + item.price * item.qty, 0);
