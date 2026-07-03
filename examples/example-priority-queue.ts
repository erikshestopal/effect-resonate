import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-priority-queue-ts";
export const functionName = "processQueue";
export const sampleArgs = [{ jobs: [{ id: "job-1", priority: "critical", description: "ship", workMs: 1 }] }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-priority-queue-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-priority-queue-ts-worker");

const Payload = Schema.Struct({
  jobs: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      priority: Schema.Literals(["critical", "high", "normal", "low"]),
      description: Schema.String,
      workMs: Schema.Number,
    }),
  ),
});
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(yield* ctx.run(Effect.logInfo(`critical:job-1:ship`).pipe(Effect.as(`critical:job-1:ship`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
