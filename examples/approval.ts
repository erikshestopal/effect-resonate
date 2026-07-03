import { BunRuntime } from "@effect/platform-bun";
import { Effect, Schema } from "effect";
import { Protocol, Resonate, ResonateContext } from "effect-resonate";
import { ResonateTest } from "effect-resonate/testing";

const Approval = Resonate.promise("approval", {
  success: Schema.Struct({ approvedBy: Schema.String }),
});

const ApprovalFlow = Resonate.function("ApprovalFlow", {
  payload: Schema.String,
});

const App = Resonate.group(ApprovalFlow);

const Handlers = App.toLayer(
  App.of({
    ApprovalFlow: () =>
      Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const approval = yield* ctx.promise(Approval);
        const result = yield* approval.await;
        return `approved:${result.approvedBy}`;
      }),
  }),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.ResonateClient;
  const id = Protocol.ExecutionId.make("approval.1");
  const handle = yield* client.beginRpc(ApprovalFlow, id, ["order-1"]);
  yield* client.resolve(Approval, Approval.id(id), { approvedBy: "erik" });
  return yield* handle.await;
}).pipe(Effect.provide(ResonateTest.layer(App, Handlers)));

if (import.meta.main) {
  BunRuntime.runMain(program);
}
