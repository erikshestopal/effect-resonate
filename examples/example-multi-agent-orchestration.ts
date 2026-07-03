import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-multi-agent-orchestration-ts";
export const functionName = "orchestrate";
export const sampleArgs = [{ topic: "resonate", crashOnWriter: false }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-multi-agent-orchestration-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-multi-agent-orchestration-ts-worker");

const Payload = Schema.Struct({ topic: Schema.String, crashOnWriter: Schema.Boolean });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`research ${input.topic}`).pipe(Effect.as(`research ${input.topic}`))),
        );
        results.push(yield* ctx.run(Effect.logInfo(`write ${input.topic}`).pipe(Effect.as(`write ${input.topic}`))));
        results.push(yield* ctx.run(Effect.logInfo(`review ${input.topic}`).pipe(Effect.as(`review ${input.topic}`))));
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
