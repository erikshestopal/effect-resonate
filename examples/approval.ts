import { BunRuntime } from "@effect/platform-bun";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Schema } from "effect";
import {
  Codec,
  DurablePromise,
  Protocol,
  Resonate,
  ResonateContext,
  ResonateSchedule,
  Task,
  Worker,
} from "effect-resonate";

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
        yield* ctx.run(
          Effect.sync(() => {
            const message = `workflow ${workflowId} waiting on ${approval.id}`;
            console.log(message);
            return message;
          }),
        );
        const result = yield* approval.await;
        return `foo workflow ${workflowId} approved by ${result.approvedBy}`;
      }),
  }),
);

const base = Layer.mergeAll(
  Resonate.layerHttp({ url, group, pid }).pipe(Layer.provide(BunHttpClient.layer)),
  BunCrypto.layer,
  Codec.ResonateEncryptor.layerNoop,
);

const services = Layer.mergeAll(
  Codec.ResonateCodec.layerJson,
  DurablePromise.DurablePromises.layer,
  Task.Tasks.layer,
  ResonateSchedule.Schedules.layer,
  handlers,
).pipe(Layer.provideMerge(base));

const client = Resonate.ResonateClient.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(ResonateContext.ExecutionEngine.layer.pipe(Layer.provideMerge(services))),
);

if (import.meta.main) {
  BunRuntime.runMain(
    Layer.launch(Worker.layer(App, { group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(client))),
  );
}
