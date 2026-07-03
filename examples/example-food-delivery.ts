import { BunRuntime } from "@effect/platform-bun";
import { Config, DateTime, Duration, Effect, HashMap, Layer, Option, Ref, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-food-delivery-ts";
export const functionName = "deliverFood";

const Order = Schema.Struct({
  id: Schema.String,
  restaurant: Schema.String,
  items: Schema.Array(Schema.String),
  customer: Schema.String,
  address: Schema.String,
});

const DeliveryResult = Schema.Struct({
  status: Schema.Literals(["success", "failed_no_driver", "failed_undeliverable"]),
  orderId: Schema.String,
  driverId: Schema.optional(Schema.String),
  completedAt: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});

const Payload = Schema.Tuple([Order, Schema.Boolean]);

export const sampleArgs = [
  {
    id: "order-food-1",
    restaurant: "Mario's Pizza",
    items: ["Margherita (large)", "Garlic bread"],
    customer: "Alice",
    address: "123 Main St",
  },
  false,
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-food-delivery-ts --func deliverFood --json-args '[{"id":"order-food-1","restaurant":"Mario'\''s Pizza","items":["Margherita (large)","Garlic bread"],"customer":"Alice","address":"123 Main St"},false]' example-food-delivery-ts-demo

class DriverConnectionLost extends Schema.TaggedErrorClass<DriverConnectionLost>()("DriverConnectionLost", {
  orderId: Schema.String,
}) {}

const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-food-delivery-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-food-delivery-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    const deliveryAttempts = yield* Ref.make(HashMap.empty<string, number>());
    const handlers = App.toLayer(
      App.of({
        [functionName]: (order, crashMidDelivery) =>
          Effect.gen(function* () {
            const ctx = yield* ResonateContext.ResonateContext;
            const orderId = order.id;

            yield* ctx.run(
              Effect.gen(function* () {
                yield* Effect.logInfo(`[order] Placing order ${order.id} at ${order.restaurant}`);
                yield* Effect.sleep(Duration.millis(300));
                yield* Effect.logInfo(`[order] Order ${order.id} confirmed by restaurant`);
                return orderId;
              }),
            );

            yield* ctx.run(
              Effect.gen(function* () {
                yield* Effect.logInfo(`[kitchen] Preparing order ${orderId}`);
                yield* Effect.sleep(Duration.millis(800));
                yield* Effect.logInfo(`[kitchen] Order ${orderId} is ready for pickup`);
              }),
            );

            const driverId = `driver-${orderId}`;
            yield* ctx.run(
              Effect.gen(function* () {
                yield* Effect.logInfo(`[dispatch] Finding driver for order ${orderId}`);
                yield* Effect.sleep(Duration.millis(400));
                yield* Effect.logInfo(`[dispatch] Driver ${driverId} assigned to order ${orderId}`);
                return driverId;
              }),
            );

            yield* ctx.run(
              Effect.gen(function* () {
                yield* Effect.logInfo(`[pickup] Driver ${driverId} picking up order ${orderId}`);
                yield* Effect.sleep(Duration.millis(400));
                yield* Effect.logInfo(`[pickup] Order ${orderId} picked up — en route to customer`);
              }),
            );

            yield* ctx.run(
              Ref.modify(deliveryAttempts, (current) => {
                const attempt = Option.getOrElse(HashMap.get(current, orderId), () => 0) + 1;
                return [attempt, HashMap.set(current, orderId, attempt)] as const;
              }).pipe(
                Effect.flatMap((attempt) =>
                  Effect.gen(function* () {
                    yield* Effect.logInfo(
                      `[delivery] Driver ${driverId} delivering order ${orderId} (attempt ${attempt})`,
                    );
                    yield* Effect.sleep(Duration.millis(300));
                    if (crashMidDelivery && attempt === 1) {
                      return yield* new DriverConnectionLost({ orderId });
                    }
                    yield* Effect.logInfo(`[delivery] Order ${orderId} delivered to customer`);
                  }),
                ),
              ),
            );

            const completedAt = DateTime.formatIso(yield* DateTime.now);
            yield* ctx.run(
              Effect.gen(function* () {
                yield* Effect.logInfo(`[complete] Completing order ${orderId}, releasing driver ${driverId}`);
                yield* Effect.sleep(Duration.millis(200));
                yield* Effect.logInfo(`[complete] Order ${orderId} done at ${completedAt}`);
                return { orderId, driverId, completedAt };
              }),
            );

            return DeliveryResult.make({ status: "success", orderId, driverId, completedAt });
          }),
      }),
    );
    return Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
