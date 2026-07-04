import { Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { ChannelResult, OrderEvent, sendEmail, sendPush, sendSlack, sendSms } from "./channels.ts";

export const repo = "example-fan-out-fan-in-ts";
export const functionName = "notifyAll";
export const sampleArgs = [
  OrderEvent.make({ orderId: "order-1", userId: "user-1", event: "created", message: "order created" }),
] as const;

export const NotificationSummary = Schema.Struct({
  orderId: Schema.String,
  channelsNotified: Schema.Finite,
  totalMs: Schema.Finite,
  results: Schema.Array(ChannelResult),
});

export const workflow = Resonate.function({ name: functionName, payload: OrderEvent });
export const App = Resonate.group(workflow);

export const handlers = App.toLayer(
  App.of({
    [functionName]: (event) =>
      Effect.gen(function* (): Effect.fn.Return<
        typeof NotificationSummary.Type,
        unknown,
        ResonateContext.ResonateContext
      > {
        const ctx = yield* ResonateContext.ResonateContext;
        const decodeChannelResult = SchemaParser.decodeUnknownEffect(ChannelResult);
        const email = yield* ctx
          .run({ name: `email-${event.orderId}`, effect: sendEmail(event) })
          .pipe(Effect.flatMap(decodeChannelResult));
        const sms = yield* ctx
          .run({ name: `sms-${event.orderId}`, effect: sendSms(event) })
          .pipe(Effect.flatMap(decodeChannelResult));
        const slack = yield* ctx
          .run({ name: `slack-${event.orderId}`, effect: sendSlack(event) })
          .pipe(Effect.flatMap(decodeChannelResult));
        const push = yield* ctx
          .run({ name: `push-${event.orderId}`, effect: sendPush(event) })
          .pipe(Effect.flatMap(decodeChannelResult));
        const results = [email, sms, slack, push];
        return NotificationSummary.make({
          orderId: event.orderId,
          channelsNotified: results.filter((result) => result.success).length,
          totalMs: results.reduce((sum, result) => sum + result.durationMs, 0),
          results,
        });
      }),
  }),
);
