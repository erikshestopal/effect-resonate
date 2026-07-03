import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-node-drain-orchestrator-ts";
export const functionName = "drainNode";
export const sampleArgs = [{ nodeId: "node-1" }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-node-drain-orchestrator-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-node-drain-orchestrator-ts-worker");

const Payload = Schema.Struct({ nodeId: Schema.String });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`cordon ${input.nodeId}`).pipe(Effect.as(`cordon ${input.nodeId}`))),
        );
        results.push(yield* ctx.run(Effect.logInfo(`drain ${input.nodeId}`).pipe(Effect.as(`drain ${input.nodeId}`))));
        results.push(
          yield* ctx.run(Effect.logInfo(`uncordon ${input.nodeId}`).pipe(Effect.as(`uncordon ${input.nodeId}`))),
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
