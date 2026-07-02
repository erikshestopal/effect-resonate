import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Duration, Effect, Layer, Schema, SchemaParser } from "effect";
import { ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as NetworkLocal from "../src/NetworkLocal.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import { Tasks } from "../src/Task.ts";

const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const Workflow = Resonate.function("Workflow", {
  payload: Schema.Number,
});

const group = Resonate.group(Workflow);

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({
    group: "workers",
    pid: "engine-1",
    tickInterval: Duration.hours(24),
    retryTimeout: Duration.seconds(5),
  }),
  BunCrypto.layer,
  ResonateEncryptor.layerNoop,
);

const codecLayer = ResonateCodec.layerJson.pipe(Layer.provide(baseLayer));
const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer).pipe(
  Layer.provide(Layer.mergeAll(baseLayer, codecLayer)),
);
const clientLayer = Resonate.ResonateClient.layer({
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("engine-1"),
  ttl: Duration.seconds(30),
}).pipe(Layer.provide(Layer.mergeAll(baseLayer, codecLayer, protocolLayer)));
const engineLayer = ExecutionEngine.layer.pipe(Layer.provide(Layer.mergeAll(protocolLayer, codecLayer)));
const layer = Layer.mergeAll(baseLayer, codecLayer, protocolLayer, clientLayer, engineLayer);

const snap = Effect.fn("ExecutionEngineTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  return response.data;
});

const acquiredRoot = Effect.fn("ExecutionEngineTest.acquiredRoot")(function* (id: Protocol.PromiseId) {
  const state = yield* snap();
  const task = state.tasks.find((task) => task.id === id);
  const promise = state.promises.find((promise) => promise.id === id);
  if (task?.state !== "acquired" || !promise) {
    return yield* Effect.die(`Root '${id}' was not acquired`);
  }
  return { task, promise };
});

describe("ExecutionEngine", () => {
  it.effect("executes local ctx.run steps with deterministic ids and root fulfillment", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const codec = yield* ResonateCodec;
      const handlers = group.toLayer(
        group.of({
          Workflow: (value) =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const stepped = yield* ctx.run(Effect.succeed(value + 1));
              return Number(stepped) + 1;
            }),
        }),
      );
      const registry = yield* group.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(Workflow, Protocol.ExecutionId.make("engine-root-1"), [1]);
      const root = yield* acquiredRoot(handle.id);
      const outcome = yield* engine.execute({ task: root.task, promise: root.promise, registry });
      expect(outcome._tag).toBe("Done");

      const state = yield* snap();
      const child = state.promises.find((promise) => promise.id === "engine-root-1.0");
      const completedRoot = state.promises.find((promise) => promise.id === handle.id);
      expect(child?.state).toBe("resolved");
      expect(child?.tags.reserved["resonate:scope"]).toBe("local");
      expect(child?.tags.reserved["resonate:parent"]).toBe(handle.id);
      expect(completedRoot?.state).toBe("resolved");
      expect(yield* codec.decode(completedRoot?.value ?? Protocol.emptyValue)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("replays a completed local step from the promise cache without re-executing it", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const engine = yield* ExecutionEngine;
      const promises = yield* DurablePromises;
      const codec = yield* ResonateCodec;
      let executions = 0;
      const handlers = group.toLayer(
        group.of({
          Workflow: () =>
            Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              const value = yield* ctx.run(
                Effect.sync(() => {
                  executions = executions + 1;
                  return 41;
                }),
              );
              return Number(value) + 1;
            }),
        }),
      );
      const registry = yield* group.registry().pipe(Effect.provide(handlers));

      const handle = yield* client.beginRun(Workflow, Protocol.ExecutionId.make("engine-root-2"), [0]);
      const root = yield* acquiredRoot(handle.id);
      const encoded = yield* codec.encode(41);
      const child = yield* promises.create({
        id: Protocol.PromiseId.make("engine-root-2.0"),
        timeoutAt: root.promise.timeoutAt,
        param: Protocol.emptyValue,
        tags: Protocol.emptyTags,
      });
      const settled = yield* promises.settle({
        id: child.id,
        state: Schema.Literal("resolved").make("resolved"),
        value: encoded,
      });

      yield* engine.execute({ task: root.task, promise: root.promise, registry, preload: [settled] });
      const state = yield* snap();
      const completedRoot = state.promises.find((promise) => promise.id === handle.id);
      expect(executions).toBe(0);
      expect(yield* codec.decode(completedRoot?.value ?? Protocol.emptyValue)).toBe(42);
    }).pipe(Effect.provide(layer)),
  );
});
