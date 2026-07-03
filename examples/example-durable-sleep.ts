import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-durable-sleep-ts";
export const functionName = "sleepingWorkflow";
export const sampleArgs = [1] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-durable-sleep-ts --func sleepingWorkflow --json-args '[1]' example-durable-sleep-ts-demo

const Payload = Schema.Number;
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
            Effect.logInfo(`Sleeping for ${input} milliseconds`).pipe(Effect.as(`Sleeping for ${input} milliseconds`)),
          ),
        );
        results.push(
          yield* ctx.run(
            Effect.logInfo(`Slept for ${input / 1000} seconds`).pipe(Effect.as(`Slept for ${input / 1000} seconds`)),
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
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-durable-sleep-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-durable-sleep-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
