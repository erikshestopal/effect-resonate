import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-ecommerce-application-ts";
export const functionName = "checkout";
export const sampleArgs = [{ orderId: "order-1", userId: "user-1", itemId: "item-1" }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-ecommerce-application-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-ecommerce-application-ts-worker");

const Payload = Schema.Struct({ orderId: Schema.String, userId: Schema.String, itemId: Schema.String });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`checkout ${input.orderId}`).pipe(Effect.as(`checkout ${input.orderId}`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`charge ${input.userId}`).pipe(Effect.as(`charge ${input.userId}`))),
        );
        results.push(yield* ctx.run(Effect.logInfo(`ship ${input.itemId}`).pipe(Effect.as(`ship ${input.itemId}`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
