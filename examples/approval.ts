import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";

const Approval = Resonate.promise({ name: "approval", success: Schema.Struct({ approvedBy: Schema.String }) });

const FooWorkflow = Resonate.function({ name: "foo-workflow", payload: Schema.String });

// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@workers --func foo-workflow --json-args '["workflow-1"]' approval-demo
const App = Resonate.group(FooWorkflow);

const handlers = App.toLayer(
  App.of({
    "foo-workflow": (workflowId) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const approval = yield* ctx.promise({ declaration: Approval });
        const message = `workflow ${workflowId} waiting on ${approval.id}`;
        yield* ctx.run({ effect: Effect.logInfo(message).pipe(Effect.as(message)) });
        const result = yield* approval.await;
        return `foo workflow ${workflowId} approved by ${result.approvedBy}`;
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault("workers"));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault("approval-worker"));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Resonate.Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(5) } }).pipe(
      Layer.provideMerge(handlers),
      Layer.provideMerge(BunHttpClient.layer),
      Layer.provideMerge(BunCrypto.layer),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
