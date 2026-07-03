import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-kafka-worker-ts";
export const functionName = "workflow";
export const sampleArgs = [{ recordId: "record-1", offset: 1 }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-kafka-worker-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-kafka-worker-ts-worker");

const Payload = Schema.Struct({ recordId: Schema.String, offset: Schema.Number });
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
            Effect.logInfo(`delete batch ${input.recordId}`).pipe(Effect.as(`delete batch ${input.recordId}`)),
          ),
        );
        results.push(
          yield* ctx.run(
            Effect.logInfo(`publish completion ${input.offset}`).pipe(Effect.as(`publish completion ${input.offset}`)),
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
