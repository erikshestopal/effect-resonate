import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-ai-image-pipeline-ts";
export const functionName = "runImagePipeline";
export const sampleArgs = [{ prompt: "cat", crashMode: "none" }] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-ai-image-pipeline-ts --func runImagePipeline --json-args '[{"prompt":"cat","crashMode":"none"}]' example-ai-image-pipeline-ts-demo

const Payload = Schema.Struct({ prompt: Schema.String, crashMode: Schema.String });
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`photorealistic ${input.prompt}`).pipe(Effect.as(`photorealistic ${input.prompt}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`cartoon ${input.prompt}`).pipe(Effect.as(`cartoon ${input.prompt}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`abstract ${input.prompt}`).pipe(Effect.as(`abstract ${input.prompt}`)),
          }),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-ai-image-pipeline-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(
      Config.withDefault("example-ai-image-pipeline-ts-worker"),
    );
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
