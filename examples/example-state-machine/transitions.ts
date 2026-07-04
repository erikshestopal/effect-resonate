import { DateTime, Duration, Effect, Schema } from "effect";
export const OrderState = Schema.Literals(["created", "confirmed", "shipped", "delivered", "cancelled", "refunded"]);
export type OrderState = typeof OrderState.Type;
export const Transition = Schema.Struct({
  orderId: Schema.String,
  from: Schema.NullOr(OrderState),
  to: OrderState,
  timestamp: Schema.String,
});
export type Transition = typeof Transition.Type;
export const transitionTo = Effect.fn("StateMachine.transitionTo")(function* (
  orderId: string,
  from: OrderState | null,
  to: OrderState,
) {
  yield* Effect.logInfo(`[${orderId}] ${from ?? "-"} -> ${to}`);
  yield* Effect.sleep(Duration.millis(10));
  return Transition.make({ orderId, from, to, timestamp: DateTime.formatIso(yield* DateTime.now) });
});
