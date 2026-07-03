import { Context, Effect, Layer, Option, Predicate, Queue, Ref, Schema, Stream } from "effect";
import type { TransportError } from "./Errors.ts";
import { decodeResponse, ResonateNetwork } from "./Network.ts";
import type { DebugState } from "./NetworkLocal.ts";
import * as Protocol from "./Protocol.ts";

const hasTaskTimeout = (state: DebugState, id: Protocol.TaskId, type: 0 | 1): boolean =>
  state.taskTimeouts.some((timeout) => timeout.id === id && timeout.type === type);

const suspendedHasCallback = (state: DebugState, id: Protocol.TaskId): boolean =>
  state.callbacks.some((callback) => callback.awaiter === id);

const suspendedHasConsumedCallback = (state: DebugState, id: Protocol.TaskId): boolean =>
  state.callbacks.some((callback) => {
    if (callback.awaiter !== id) {
      return false;
    }
    const awaited = state.promises.find((promise) => promise.id === callback.awaited);
    return Predicate.isUndefined(awaited) || awaited.state !== "pending";
  });

export const assertInvariants = Effect.fn("assertInvariants")(function* (state: DebugState) {
  for (const task of state.tasks) {
    const promise = state.promises.find((promise) => promise.id === task.id);
    if (Predicate.isUndefined(promise)) {
      return yield* Effect.die(`Invariant failed: task ${task.id} has no promise`);
    }
    if (task.state === "pending" && !hasTaskTimeout(state, task.id, 0)) {
      return yield* Effect.die(`Invariant failed: pending task ${task.id} has no retry timeout`);
    }
    if (task.state === "acquired" && !hasTaskTimeout(state, task.id, 1)) {
      return yield* Effect.die(`Invariant failed: acquired task ${task.id} has no lease`);
    }
    if (task.state === "suspended" && !suspendedHasCallback(state, task.id)) {
      return yield* Effect.die(`Invariant failed: suspended task ${task.id} has no callback`);
    }
    if (task.state === "suspended" && suspendedHasConsumedCallback(state, task.id)) {
      return yield* Effect.die(`Invariant failed: suspended task ${task.id} has a consumed callback`);
    }
    if (task.state === "suspended" && state.taskTimeouts.some((timeout) => timeout.id === task.id)) {
      return yield* Effect.die(`Invariant failed: suspended task ${task.id} has a timeout`);
    }
    if (task.state === "fulfilled" && state.taskTimeouts.some((timeout) => timeout.id === task.id)) {
      return yield* Effect.die(`Invariant failed: fulfilled task ${task.id} has a timeout`);
    }
  }
});

export type TestNetworkHandler = (request: Protocol.Request) => Effect.Effect<Protocol.Response, TransportError>;

export interface TestNetworkService {
  readonly push: (message: Protocol.Message) => Effect.Effect<void>;

  readonly requests: Effect.Effect<ReadonlyArray<Protocol.Request>>;
}

export class TestNetwork extends Context.Service<TestNetwork, TestNetworkService>()(
  "effect-resonate/testing/TestNetwork",
) {
  static layer(
    handler: TestNetworkHandler,
    options?: { readonly group?: string; readonly pid?: string },
  ): Layer.Layer<TestNetwork | ResonateNetwork> {
    return Layer.unwrap(
      Effect.gen(function* () {
        const group = Protocol.WorkerGroup.make(options?.group ?? "default");
        const pid = Protocol.ProcessId.make(options?.pid ?? "test-pid");
        const queue = yield* Queue.unbounded<Protocol.Message>();
        const seen = yield* Ref.make<ReadonlyArray<Protocol.Request>>([]);

        const network = ResonateNetwork.of({
          send: Effect.fn("TestNetwork.send")(function* (request) {
            yield* Ref.update(seen, (list) => [...list, request]);
            const response = yield* handler(request);
            const wire = yield* Effect.orDie(Schema.encodeUnknownEffect(Protocol.ResponseFromWire)(response));
            return yield* decodeResponse(request)(wire);
          }),
          messages: Stream.fromQueue(queue),
          match: (target) => Protocol.TargetAddress.pollAny(target),
          unicast: Protocol.TargetAddress.pollUni(group, pid),

          anycast: (target) => Protocol.TargetAddress.pollAny(target, Option.some(pid)),
        });

        const test = TestNetwork.of({
          push: (message) => Effect.asVoid(Queue.offer(queue, message)),
          requests: Ref.get(seen),
        });

        return Layer.mergeAll(Layer.succeed(ResonateNetwork, network), Layer.succeed(TestNetwork, test));
      }),
    );
  }
}
