import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-food-delivery-ts";
export const functionName = "deliverFood";
export const sampleArgs = [{ orderId: "food-1", hasDriver: true }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-food-delivery-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-food-delivery-ts-worker");

const Payload = Schema.Struct({ orderId: Schema.String, hasDriver: Schema.Boolean });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`placed ${input.orderId}`).pipe(Effect.as(`placed ${input.orderId}`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`prepared ${input.orderId}`).pipe(Effect.as(`prepared ${input.orderId}`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`delivered ${input.orderId}`).pipe(Effect.as(`delivered ${input.orderId}`))),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
