import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-quickstart-ts";
export const functionName = "countdown";
export const sampleArgs = [5, 1] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-quickstart-ts --func countdown --json-args '[5,1]' example-quickstart-ts-demo

const Payload = Schema.Tuple([Schema.Finite, Schema.Finite]);
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (count, delay) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        for (let i = count; i > 0; i -= 1) {
          yield* ctx.run(Effect.logInfo(`Countdown: ${i}`));
          yield* ctx.sleep(Duration.seconds(delay));
        }
        yield* ctx.run(Effect.logInfo("Done!"));
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(30) }).pipe(Layer.provideMerge(handlers));
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
