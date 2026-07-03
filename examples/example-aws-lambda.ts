import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-aws-lambda-ts";
export const functionName = "processDocument";
export const sampleArgs = [
  { jobId: "job-1", documentUrl: "https://example.com/doc.pdf", requesterId: "user-1", type: "pdf" },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-aws-lambda-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-aws-lambda-ts-worker");

const Payload = Schema.Struct({
  jobId: Schema.String,
  documentUrl: Schema.String,
  requesterId: Schema.String,
  type: Schema.String,
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
            Effect.logInfo(`download ${input.documentUrl}`).pipe(Effect.as(`download ${input.documentUrl}`)),
          ),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`analyze ${input.jobId}`).pipe(Effect.as(`analyze ${input.jobId}`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`notify ${input.requesterId}`).pipe(Effect.as(`notify ${input.requesterId}`))),
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
