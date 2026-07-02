/**
 * Test harness exports (TestClock-driven local server, simulator).
 *
 * See `docs/DESIGN.md` §4.11 (Testing).
 */
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

/** Seven structural task invariants from `docs/DESIGN.md` §8. */
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

/** Answers each request the network sends; runs in send order. */
export type TestNetworkHandler = (request: Protocol.Request) => Effect.Effect<Protocol.Response, TransportError>;

/**
 * A scripted `ResonateNetwork` stub for unit-testing layers 2–4 without the
 * local server — the role of the Rust SDK's `StubNetwork`.
 *
 * Responses produced by the handler are round-tripped through the wire schemas
 * and the shared envelope checks, so a stubbed exchange exercises exactly the
 * same decode/validate path as a real transport.
 */
export class TestNetwork extends Context.Service<
  TestNetwork,
  {
    /** Push a server message onto the network's message stream. */
    readonly push: (message: Protocol.Message) => Effect.Effect<void>;
    /** Requests observed by the scripted handler, in send order. */
    readonly requests: Effect.Effect<ReadonlyArray<Protocol.Request>>;
  }
>()("effect-resonate/testing/TestNetwork") {
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
          match: (target) =>
            Protocol.TargetAddress.make({
              transport: "poll",
              cast: "any",
              group: target,
              id: Option.none(),
            }),
          unicast: Protocol.TargetAddress.make({
            transport: "poll",
            cast: "uni",
            group,
            id: Option.some(pid),
          }),
          // Native derives its anycast address with the process id attached.
          anycast: (target) =>
            Protocol.TargetAddress.make({
              transport: "poll",
              cast: "any",
              group: target,
              id: Option.some(pid),
            }),
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
