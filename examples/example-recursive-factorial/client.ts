import { Clock, Effect, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";
import { factorial } from "./workflow.ts";

export const invokeFactorial = Effect.fn("RecursiveFactorialClient.invokeFactorial")(function* (
  n: number,
): Effect.fn.Return<number, unknown, Resonate.Client> {
  const client = yield* Resonate.Client;
  const millis = yield* Clock.currentTimeMillis;
  const handle = yield* client.beginRpc({
    targetFunction: factorial,
    executionId: Protocol.ExecutionId.make(`factorial-${n}-${millis}`),
    args: [n],
    options: { target: Protocol.WorkerGroup.make("example-recursive-factorial-ts") },
  });
  return yield* handle.await.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
});
