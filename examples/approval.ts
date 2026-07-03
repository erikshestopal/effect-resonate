import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "workers");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "approval-worker");

const Approval = Resonate.promise("approval", {
  success: Schema.Struct({ approvedBy: Schema.String }),
});

const FooWorkflow = Resonate.function("foo-workflow", {
  payload: Schema.String,
});

const App = Resonate.group(FooWorkflow);

const handlers = App.toLayer(
  App.of({
    "foo-workflow": (workflowId) =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const approval = yield* ctx.promise(Approval);
        const message = `workflow ${workflowId} waiting on ${approval.id}`;
        yield* ctx.run(Effect.logInfo(message).pipe(Effect.as(message)));
        const result = yield* approval.await;
        return `foo workflow ${workflowId} approved by ${result.approvedBy}`;
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
