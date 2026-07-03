import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-aws-lambda-ts";
export const functionName = "processDocument";
export const sampleArgs = [
  { jobId: "job-1", documentUrl: "https://example.com/doc.pdf", requesterId: "user-1", type: "pdf" },
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-aws-lambda-ts --func processDocument --json-args '[{"jobId":"job-1","documentUrl":"https://example.com/doc.pdf","requesterId":"user-1","type":"pdf"}]' example-aws-lambda-ts-demo

const Payload = Schema.Struct({
  jobId: Schema.String,
  documentUrl: Schema.String,
  requesterId: Schema.String,
  type: Schema.String,
});
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
            effect: Effect.logInfo(`download ${input.documentUrl}`).pipe(Effect.as(`download ${input.documentUrl}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`analyze ${input.jobId}`).pipe(Effect.as(`analyze ${input.jobId}`)),
          }),
        );
        results.push(
          yield* ctx.run({
            effect: Effect.logInfo(`notify ${input.requesterId}`).pipe(Effect.as(`notify ${input.requesterId}`)),
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
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-aws-lambda-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("example-aws-lambda-ts-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
