import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-express-integration-ts";
export const functionName = "processOrder";
export const sampleArgs = [{ id: "order-1", itemId: "item-1", quantity: 1 }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-express-integration-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-express-integration-ts-worker");

const Payload = Schema.Struct({ id: Schema.String, itemId: Schema.String, quantity: Schema.Number });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`validated order ${input.id}`).pipe(Effect.as(`validated order ${input.id}`))),
        );
        results.push(
          yield* ctx.run(
            Effect.logInfo(`reserved inventory ${input.itemId}`).pipe(Effect.as(`reserved inventory ${input.itemId}`)),
          ),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`charged order ${input.id}`).pipe(Effect.as(`charged order ${input.id}`))),
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
