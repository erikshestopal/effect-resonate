import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-hello-world-ts";
export const functionName = "foo";
export const sampleArgs = ["World"] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-hello-world-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-hello-world-ts-worker");

const Payload = Schema.String;
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(Effect.logInfo(`Hello ${input} from foo`).pipe(Effect.as(`Hello ${input} from foo`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`Hello ${input} from bar`).pipe(Effect.as(`Hello ${input} from bar`))),
        );
        results.push(
          yield* ctx.run(Effect.logInfo(`Hello ${input} from baz`).pipe(Effect.as(`Hello ${input} from baz`))),
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
