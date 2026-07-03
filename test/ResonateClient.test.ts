import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Duration, Effect, Exit, Layer, Option, Predicate, Schema, SchemaParser } from "effect";
import { currentCodec } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { DurablePromiseCanceled } from "../src/Errors.ts";
import { ResonateNetwork } from "../src/network/network.ts";
import { makeRequestHead } from "../src/testing.ts";
import * as NetworkLocal from "../src/network/local.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import * as RetryPolicy from "../src/RetryPolicy.ts";
import { Tasks } from "../src/Task.ts";

const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const Checkout = Resonate.function("Checkout", {
  payload: Schema.Struct({ id: Schema.String }),
});

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({
    group: "workers",
    pid: "client-1",
    tickInterval: Duration.hours(24),
    retryTimeout: Duration.seconds(5),
  }),
  BunCrypto.layer,
);

const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer).pipe(Layer.provide(baseLayer));
const clientLayer = Resonate.ResonateClient.layer({
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("client-1"),
  ttl: Duration.seconds(30),
}).pipe(Layer.provide(baseLayer));
const layer = Layer.mergeAll(baseLayer, protocolLayer, clientLayer);

const snap = Effect.fn("ResonateClientTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  return response.data;
});

describe("ResonateClient", () => {
  it.effect("beginRpc creates a target-tagged promise and handle await decodes the value", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const codec = yield* currentCodec;
      const promises = yield* DurablePromises;
      const handle = yield* client.beginRpc(Checkout, Protocol.ExecutionId.make("rpc-1"), [{ id: "order-1" }], {
        target: Protocol.WorkerGroup.make("payments"),
      });

      const state = yield* snap();
      const promise = state.promises.find((promise) => promise.id === handle.id);
      expect(promise?.state).toBe("pending");
      expect(promise?.tags.reserved["resonate:target"]?.address).toBe("local://any@payments");
      expect(state.tasks.find((task) => task.id === handle.id)?.state).toBe("pending");

      const invocation = yield* codec.decode(promise?.param ?? Protocol.emptyValue);
      expect(invocation).toEqual({ func: "Checkout", args: [{ id: "order-1" }], version: 1 });

      expect(Option.isNone(yield* handle.poll)).toBe(true);
      const encoded = yield* codec.encode("ok");
      yield* promises.settle({ id: handle.id, state: Schema.Literal("resolved").make("resolved"), value: encoded });
      expect(yield* handle.await).toBe("ok");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("beginRun creates an acquired root task with self-referential root tags", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRun(Checkout, Protocol.ExecutionId.make("run-1"), [{ id: "order-2" }]);

      const state = yield* snap();
      const promise = state.promises.find((promise) => promise.id === handle.id);
      const task = state.tasks.find((task) => task.id === handle.id);
      expect(task?.state).toBe("acquired");
      expect(promise?.tags.reserved["resonate:origin"]).toBe(handle.id);
      expect(promise?.tags.reserved["resonate:prefix"]).toBe(handle.id);
      expect(promise?.tags.reserved["resonate:branch"]).toBe(handle.id);
      expect(promise?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(promise?.tags.reserved["resonate:scope"]).toBe("global");
      expect(promise?.tags.reserved["resonate:target"]?.address).toBe("local://any@workers/client-1");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("handle cancel settles rejected_canceled and await maps to DurablePromiseCanceled", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRpc(Checkout, Protocol.ExecutionId.make("cancel-1"), [{ id: "order-3" }]);
      yield* handle.cancel;

      const exit = yield* handle.await.pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
      const error = yield* Effect.flip(handle.await);
      expect(Predicate.isTagged(error, "DurablePromiseCanceled")).toBe(true);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("string-name rpc encodes raw positional args with default version", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const codec = yield* currentCodec;
      const handle = yield* client.beginRpc("RemoteCheckout", Protocol.ExecutionId.make("string-rpc-1"), [1, "two"]);

      const state = yield* snap();
      const promise = state.promises.find((promise) => promise.id === handle.id);
      const invocation = yield* codec.decode(promise?.param ?? Protocol.emptyValue);
      expect(invocation).toEqual({ func: "RemoteCheckout", args: [1, "two"], version: 1 });
      expect(promise?.tags.reserved["resonate:target"]?.address).toBe("local://any@workers");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("persists retry policy in invocation params", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const codec = yield* currentCodec;
      const handle = yield* client.beginRun(Checkout, Protocol.ExecutionId.make("retry-param-1"), [{ id: "order-4" }], {
        retryPolicy: RetryPolicy.linear({ delay: Duration.seconds(2), maxRetries: 7 }),
      });

      const state = yield* snap();
      const promise = state.promises.find((promise) => promise.id === handle.id);
      const invocation = yield* codec.decode(promise?.param ?? Protocol.emptyValue);
      expect(invocation).toEqual({
        func: "Checkout",
        args: [{ id: "order-4" }],
        version: 1,
        retry: RetryPolicy.linear({ delay: Duration.seconds(2), maxRetries: 7 }),
      });
    }).pipe(Effect.provide(layer)),
  );
});
