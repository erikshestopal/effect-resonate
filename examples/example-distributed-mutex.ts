import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-distributed-mutex-ts";
export const functionName = "exclusiveResourceAccess";
export const sampleArgs = [{ resource: "resource-1", workers: ["worker-a", "worker-b"], shouldCrash: false }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-distributed-mutex-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-distributed-mutex-ts-worker");

const Payload = Schema.Struct({
  resource: Schema.String,
  workers: Schema.Array(Schema.String),
  shouldCrash: Schema.Boolean,
});
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
            Effect.logInfo(`${input.workers.join(",")} accessed ${input.resource}`).pipe(
              Effect.as(`${input.workers.join(",")} accessed ${input.resource}`),
            ),
          ),
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
