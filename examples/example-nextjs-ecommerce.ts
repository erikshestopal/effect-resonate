import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-nextjs-ecommerce-ts";
export const functionName = "oneClickBuy";
export const sampleArgs = [{ itemId: "item-1", cancelKey: "cancel-1" }] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-nextjs-ecommerce-ts --func oneClickBuy --json-args '[{"itemId":"item-1","cancelKey":"cancel-1"}]' example-nextjs-ecommerce-ts-demo

const Payload = Schema.Struct({ itemId: Schema.String, cancelKey: Schema.String });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(
            Effect.logInfo(`one click buy ${input.itemId}`).pipe(Effect.as(`one click buy ${input.itemId}`)),
          ),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-nextjs-ecommerce-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-nextjs-ecommerce-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
