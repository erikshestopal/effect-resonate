import { Effect, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";
import { foo, qux, zim } from "./workflow.ts";

export const awaitChain = Effect.fn("AsyncRpcGateway.awaitChain")(function* (): Effect.fn.Return<
  number,
  unknown,
  Resonate.Client
> {
  const client = yield* Resonate.Client;
  const handle = yield* client.beginRpc({
    targetFunction: foo,
    executionId: Protocol.ExecutionId.make("await-chain"),
    args: ["foo"],
    options: { target: Protocol.WorkerGroup.make("service-a") },
  });
  return yield* handle.await.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
});

export const detachedChain = Effect.fn("AsyncRpcGateway.detachedChain")(function* (): Effect.fn.Return<
  string,
  unknown,
  Resonate.Client
> {
  const client = yield* Resonate.Client;
  yield* client.beginRpc({
    targetFunction: qux,
    executionId: Protocol.ExecutionId.make("detached-chain"),
    args: [1],
    options: { target: Protocol.WorkerGroup.make("service-d") },
  });
  return "detached-chain started";
});

export const fanOutWorkflow = Effect.fn("AsyncRpcGateway.fanOutWorkflow")(function* (): Effect.fn.Return<
  number,
  unknown,
  Resonate.Client
> {
  const client = yield* Resonate.Client;
  const handle = yield* client.beginRpc({
    targetFunction: zim,
    executionId: Protocol.ExecutionId.make("fan-out-workflow"),
    args: [1],
    options: { target: Protocol.WorkerGroup.make("service-g") },
  });
  return yield* handle.await.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
});
