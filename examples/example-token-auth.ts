import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-token-auth-ts";
export const functionName = "workflow";
export const sampleArgs = ["hello"] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-token-auth-ts --func workflow --json-args '["hello"]' example-token-auth-ts-demo

const Payload = Schema.String;
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (greeting) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        const message = `${greeting} world!`;
        yield* ctx.run({ effect: Effect.logInfo(message) });
        return message;
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
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
