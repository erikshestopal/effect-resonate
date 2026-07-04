import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";
import { ComputeRequest } from "./client.ts";
export const repo = "example-load-balancing-ts";
export const functionName = "computeSomething";
export const sampleArgs = [{ id: "compute-1", computeCost: 7 }] as const;
export const ComputeResult = Schema.Struct({
  id: Schema.String,
  computeCost: Schema.Finite,
  workerGroup: Schema.String,
});
export const workflow = Resonate.function({ name: functionName, payload: ComputeRequest });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof ComputeResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run({
          name: `compute-${input.id}`,
          effect: Effect.logInfo(`${input.id} computed with cost ${input.computeCost}`),
        });
        return ComputeResult.make({ id: input.id, computeCost: input.computeCost, workerGroup: "workers" });
      }),
  }),
);
export const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);
