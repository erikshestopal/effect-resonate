import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-ai-image-pipeline-ts";
export const functionName = "runImagePipeline";
export const sampleArgs = [{ prompt: "cat", crashMode: "none" }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-ai-image-pipeline-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-ai-image-pipeline-ts-worker");

const Payload = Schema.Struct({ prompt: Schema.String, crashMode: Schema.String });
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
            Effect.logInfo(`photorealistic ${input.prompt}`).pipe(Effect.as(`photorealistic ${input.prompt}`)),
          ),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`cartoon ${input.prompt}`).pipe(Effect.as(`cartoon ${input.prompt}`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`abstract ${input.prompt}`).pipe(Effect.as(`abstract ${input.prompt}`))),
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
