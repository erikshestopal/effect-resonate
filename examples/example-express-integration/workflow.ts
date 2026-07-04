import { Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import {
  Order,
  OrderResult,
  chargePayment,
  orderTotal,
  reserveInventory,
  sendConfirmation,
  validateOrder,
} from "./handlers.ts";

export const repo = "example-express-integration-ts";
export const functionName = "processOrder";
export const sampleArgs = [
  {
    id: "order-1",
    items: [{ sku: "widget-pro", qty: 2, price: 2999 }],
    customer: { id: "cus-1", email: "alice@example.com" },
  },
] as const;

export const workflow = Resonate.function({ name: functionName, payload: Order });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (order) =>
      Effect.gen(function* (): Effect.fn.Return<typeof OrderResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: `validate-${order.id}`, effect: validateOrder(order) });
        yield* ctx.run({ name: `reserve-${order.id}`, effect: reserveInventory(order) });
        const chargeId = yield* ctx
          .run({ name: `charge-${order.id}`, effect: chargePayment(order) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        yield* ctx.run({ name: `confirm-${order.id}`, effect: sendConfirmation(order, chargeId) });
        return OrderResult.make({
          orderId: order.id,
          inventoryReserved: true,
          paymentCharged: true,
          confirmationSent: true,
          total: orderTotal(order),
          chargeId,
        });
      }),
  }),
);
