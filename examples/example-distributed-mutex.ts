import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-distributed-mutex-ts";
export const functionName = "exclusiveResourceAccess";
export const sampleArgs = [{ resource: "resource-1", workers: ["worker-a", "worker-b"], shouldCrash: false }] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-distributed-mutex-ts --func exclusiveResourceAccess --json-args '[{"resource":"resource-1","workers":["worker-a","worker-b"],"shouldCrash":false}]' example-distributed-mutex-ts-demo

const Payload = Schema.Struct({
  resource: Schema.String,
  workers: Schema.Array(Schema.String),
  shouldCrash: Schema.Boolean,
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
            effect: Effect.logInfo(`${input.workers.join(",")} accessed ${input.resource}`).pipe(
              Effect.as(`${input.workers.join(",")} accessed ${input.resource}`),
            ),
          }),
        );
        yield* ctx.sleep({ for: Duration.millis(1) });
        return { repo, functionName, results };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("example-distributed-mutex-ts"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(
      Config.withDefault("example-distributed-mutex-ts-worker"),
    );
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
