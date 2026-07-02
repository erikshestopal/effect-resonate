import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Duration, Effect, Layer, Schema, SchemaParser } from "effect";
import { TestClock } from "effect/testing";
import { ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import { makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as NetworkLocal from "../src/NetworkLocal.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ExecutionEngine, ResonateContext } from "../src/ResonateContext.ts";
import { Tasks } from "../src/Task.ts";
import * as Worker from "../src/Worker.ts";

const Workflow = Resonate.function("WorkerWorkflow", {
  payload: Schema.Number,
});

const Blocking = Resonate.function("BlockingWorkflow", {
  payload: Schema.Number,
});

const group = Resonate.group(Workflow, Blocking);
const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapResponse.members[0]);

const baseLayer = Layer.mergeAll(
  NetworkLocal.layer({
    group: "workers",
    pid: "worker-1",
    tickInterval: Duration.hours(24),
    retryTimeout: Duration.seconds(5),
  }),
  BunCrypto.layer,
  ResonateEncryptor.layerNoop,
);
const codecLayer = ResonateCodec.layerJson;
const protocolLayer = Layer.mergeAll(DurablePromises.layer, Tasks.layer);
const clientLayer = Resonate.ResonateClient.layer({
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("client-1"),
  ttl: Duration.seconds(30),
});
const engineLayer = ExecutionEngine.layer;
const handlerLayer = group.toLayer(
  group.of({
    WorkerWorkflow: (value) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
        const ctx = yield* ResonateContext;
        const stepped = yield* ctx.run(Effect.succeed(value + 1));
        return Number(stepped) + 1;
      }),
    BlockingWorkflow: () => Effect.never,
  }),
);
const workerLayer = Worker.layer(group, {
  group: Protocol.WorkerGroup.make("workers"),
  pid: Protocol.ProcessId.make("worker-1"),
  ttl: Duration.seconds(30),
});
const coreServicesLayer = Layer.mergeAll(codecLayer, protocolLayer, handlerLayer).pipe(Layer.provideMerge(baseLayer));
const engineServicesLayer = engineLayer.pipe(Layer.provideMerge(coreServicesLayer));
const clientServicesLayer = clientLayer.pipe(Layer.provideMerge(engineServicesLayer));
const layer = workerLayer.pipe(Layer.provideMerge(clientServicesLayer));

const snap = Effect.fn("WorkerTest.snap")(function* () {
  const network = yield* ResonateNetwork;
  const response = yield* network.send(Protocol.DebugSnapRequest.make({ head: yield* makeRequestHead, data: {} }));
  if (!isDebugSnapSuccess(response)) {
    return yield* Effect.die(response.data);
  }
  return response.data;
});

const tick = Effect.fn("WorkerTest.tick")(function* (millis: number) {
  const network = yield* ResonateNetwork;
  yield* network.send(
    Protocol.DebugTickRequest.make({
      head: yield* makeRequestHead,
      data: { time: Schema.decodeUnknownSync(Protocol.Timestamp)(millis) },
    }),
  );
});

describe("Worker", () => {
  it.effect("acquires execute messages and fulfills client RPCs", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const promises = yield* DurablePromises;
      const codec = yield* ResonateCodec;
      const handle = yield* client.beginRpc(Workflow, Protocol.ExecutionId.make("worker-root-1"), [1]);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const promise = yield* promises.get(handle.id);
      expect(promise.state).toBe("resolved");
      expect(yield* codec.decode(promise.value)).toBe(3);
    }).pipe(Effect.provide(layer)),
  );

  it.effect("heartbeats the actual held task/version so leases stay acquired", () =>
    Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRpc(Blocking, Protocol.ExecutionId.make("worker-heartbeat-1"), [0]);
      yield* Effect.yieldNow;
      yield* TestClock.adjust(Duration.seconds(20));
      yield* tick(31_000);

      const state = yield* snap();
      const task = state.tasks.find((task) => task.id === handle.id);
      expect(task?.state).toBe("acquired");
    }).pipe(Effect.provide(layer)),
  );
});
