import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-mcp-tools-ts";
export const functionName = "getForecast";
export const sampleArgs = [37.7749, -122.4194] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-mcp-tools-ts --func getForecast --json-args '[37.7749,-122.4194]' example-mcp-tools-ts-demo

const Payload = Schema.Tuple([Schema.Number, Schema.Number]);
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (latitude, longitude) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(Effect.logInfo("fetched forecast point"));
        yield* ctx.sleep(Duration.seconds(1));
        yield* ctx.run(Effect.logInfo("fetched detailed forecast"));
        return `Today: 65°F, 10 mph W. Forecast for ${latitude},${longitude}`;
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
