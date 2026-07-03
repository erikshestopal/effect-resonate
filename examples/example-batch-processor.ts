import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-batch-processor-ts";
export const functionName = "importRecords";
export const sampleArgs = [
  {
    records: [
      { id: "a", value: 1 },
      { id: "b", value: 2 },
    ],
    batchSize: 1,
  },
] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-batch-processor-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-batch-processor-ts-worker");

const Payload = Schema.Struct({
  records: Schema.Array(Schema.Struct({ id: Schema.String, value: Schema.Number })),
  batchSize: Schema.Number,
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
            Effect.logInfo(`processed ${input.records.length} records`).pipe(
              Effect.as(`processed ${input.records.length} records`),
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
