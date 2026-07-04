import { Duration, Effect, Schema } from "effect";

export const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  userId: Schema.String,
  event: Schema.String,
  message: Schema.String,
  simulateCrash: Schema.optional(Schema.Boolean),
});
export type OrderEvent = typeof OrderEvent.Type;

export const ChannelResult = Schema.Struct({
  channel: Schema.String,
  success: Schema.Boolean,
  messageId: Schema.String,
  durationMs: Schema.Finite,
});
export type ChannelResult = typeof ChannelResult.Type;

const deliver = Effect.fn("FanOutFanIn.deliver")(function* (channel: string, millis: number, event: OrderEvent) {
  yield* Effect.logInfo(`[${channel}] notifying ${event.userId} about ${event.orderId}`);
  yield* Effect.sleep(Duration.millis(millis));
  return ChannelResult.make({
    channel,
    success: true,
    messageId: `${channel}-${event.orderId}`,
    durationMs: millis,
  });
});

export const sendEmail = (event: OrderEvent) => deliver("email", 40, event);
export const sendSms = (event: OrderEvent) => deliver("sms", 25, event);
export const sendSlack = (event: OrderEvent) => deliver("slack", 18, event);
export const sendPush = (event: OrderEvent) => deliver("push", 12, event);
