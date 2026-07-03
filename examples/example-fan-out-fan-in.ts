import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-fan-out-fan-in-ts";
export const functionName = "notifyAll";
export const sampleArgs = [
  { orderId: "order-1", userId: "user-1", event: "created", message: "order created" },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-fan-out-fan-in-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-fan-out-fan-in-ts-worker");

const Payload = Schema.Struct({
  orderId: Schema.String,
  userId: Schema.String,
  event: Schema.String,
  message: Schema.String,
});
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(yield* ctx.run(Effect.logInfo(`email ${input.userId}`).pipe(Effect.as(`email ${input.userId}`))));
        results.push(yield* ctx.run(Effect.logInfo(`sms ${input.userId}`).pipe(Effect.as(`sms ${input.userId}`))));
        results.push(
          yield* ctx.run(Effect.logInfo(`slack ${input.orderId}`).pipe(Effect.as(`slack ${input.orderId}`))),
        );
        results.push(yield* ctx.run(Effect.logInfo(`push ${input.orderId}`).pipe(Effect.as(`push ${input.orderId}`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
