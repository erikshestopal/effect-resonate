import { BunRuntime } from "@effect/platform-bun";
import { Config, DateTime, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-node-drain-orchestrator-ts";
export const functionName = "drainAllNodes";
export const sampleArgs = [
  "drain-demo",
  {
    evictionTimeout: 60000,
    drainTimeout: 300000,
    ignoreDaemonSets: true,
    deleteLocalData: true,
    force: false,
    gracePeriod: 30,
  },
  { pool: "workers" },
] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-node-drain-orchestrator-ts --func drainAllNodes --json-args '["drain-demo",{"evictionTimeout":60000,"drainTimeout":300000,"ignoreDaemonSets":true,"deleteLocalData":true,"force":false,"gracePeriod":30},{"pool":"workers"}]' example-node-drain-orchestrator-ts-demo

const Payload = Schema.Tuple([
  Schema.String,
  Schema.Struct({
    evictionTimeout: Schema.Finite,
    drainTimeout: Schema.Finite,
    ignoreDaemonSets: Schema.Boolean,
    deleteLocalData: Schema.Boolean,
    force: Schema.Boolean,
    gracePeriod: Schema.Finite,
  }),
  Schema.Struct({ pool: Schema.String }),
]);
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (operationId, options, nodeSelector) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        const startedAt = DateTime.formatIso(yield* DateTime.now);
        const results: Array<unknown> = [];
        for (const node of [nodeSelector.pool]) {
          yield* ctx.run({ effect: Effect.logInfo(`[Drain] Cordoning node ${node}`) });
          yield* ctx.run({ effect: Effect.logInfo(`[Drain] Getting pods on node ${node}`) });
          yield* ctx.run({ effect: Effect.logInfo(`[Drain] Evicting pods on ${node}`) });
          results.push({
            node,
            success: options.force || node.length > 0,
            startedAt,
            completedAt: DateTime.formatIso(yield* DateTime.now),
            podsEvicted: 0,
          });
          yield* ctx.run({ effect: Effect.logInfo(`[Drain] Uncordoning node ${node}`) });
        }
        return {
          operationId,
          status: "completed",
          startedAt,
          completedAt: DateTime.formatIso(yield* DateTime.now),
          nodes: results,
        };
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
