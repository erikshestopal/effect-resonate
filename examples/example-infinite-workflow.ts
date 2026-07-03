import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-infinite-workflow-ts";
export const functionName = "healthMonitor";
export const sampleArgs = [{ services: ["api", "db"], intervalMs: 1, maxIterations: 2 }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-infinite-workflow-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-infinite-workflow-ts-worker");

const Payload = Schema.Struct({
  services: Schema.Array(Schema.String),
  intervalMs: Schema.Number,
  maxIterations: Schema.Number,
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
            Effect.logInfo(`health ${input.services.join(",")}`).pipe(Effect.as(`health ${input.services.join(",")}`)),
          ),
        );
        results.push(yield* ctx.run(Effect.logInfo(`monitor complete`).pipe(Effect.as(`monitor complete`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
