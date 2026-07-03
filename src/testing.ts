import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import {
  Context,
  Crypto,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Predicate,
  Queue,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { DurablePromises } from "./DurablePromise.ts";
import type { TransportError } from "./Errors.ts";
import { decodeResponse, ResonateNetwork } from "./network/network.ts";
import * as NetworkLocal from "./network/local.ts";
import type { DebugState } from "./network/local.ts";
import * as Protocol from "./Protocol.ts";
import * as Resonate from "./Resonate.ts";
import { Schedules } from "./Schedule.ts";
import { Tasks } from "./Task.ts";
import * as Worker from "./Worker.ts";

export const assertInvariants = Effect.fn("assertInvariants")(function* (state: DebugState) {
  for (const task of state.tasks) {
    const promise = state.promises.find((promise) => promise.id === task.id);
    if (Predicate.isUndefined(promise)) {
      return yield* Effect.die(`Invariant failed: task ${task.id} has no promise`);
    }
    if (
      task.state === "pending" &&
      !state.taskTimeouts.some((timeout) => timeout.id === task.id && timeout.type === 0)
    ) {
      return yield* Effect.die(`Invariant failed: pending task ${task.id} has no retry timeout`);
    }
    if (
      task.state === "acquired" &&
      !state.taskTimeouts.some((timeout) => timeout.id === task.id && timeout.type === 1)
    ) {
      return yield* Effect.die(`Invariant failed: acquired task ${task.id} has no lease`);
    }
    if (task.state === "suspended" && !state.callbacks.some((callback) => callback.awaiter === task.id)) {
      return yield* Effect.die(`Invariant failed: suspended task ${task.id} has no callback`);
    }
    if (
      task.state === "suspended" &&
      state.callbacks.some((callback) => {
        if (callback.awaiter !== task.id) {
          return false;
        }
        const awaited = state.promises.find((promise) => promise.id === callback.awaited);
        return Predicate.isUndefined(awaited) || awaited.state !== "pending";
      })
    ) {
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

export const makeRequestHead: Effect.Effect<Protocol.RequestHead, never, Crypto.Crypto> = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
  return Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion });
});

export type TestNetworkHandler = (request: Protocol.Request) => Effect.Effect<Protocol.Response, TransportError>;

export interface TestNetworkService {
  readonly push: (message: Protocol.Message) => Effect.Effect<void>;

  readonly requests: Effect.Effect<ReadonlyArray<Protocol.Request>>;
}

export class TestNetwork extends Context.Service<TestNetwork, TestNetworkService>()(
  "effect-resonate/testing/TestNetwork",
) {
  static layer(options: {
    readonly handler: TestNetworkHandler;
    readonly group?: string;
    readonly pid?: string;
  }): Layer.Layer<TestNetwork | ResonateNetwork> {
    return Layer.unwrap(
      Effect.gen(function* () {
        const group = Protocol.WorkerGroup.make(options.group ?? "default");
        const pid = Protocol.ProcessId.make(options.pid ?? "test-pid");
        const queue = yield* Queue.unbounded<Protocol.Message>();
        const seen = yield* Ref.make<ReadonlyArray<Protocol.Request>>([]);

        const network = ResonateNetwork.of({
          send: Effect.fn("TestNetwork.send")(function* (request) {
            yield* Ref.update(seen, (list) => [...list, request]);
            const response = yield* options.handler(request);
            const wire = yield* Effect.orDie(Schema.encodeUnknownEffect(Protocol.ResponseFromWire)(response));
            return yield* decodeResponse(request)(wire);
          }),
          messages: Stream.fromQueue(queue),
          match: (target) => Protocol.TargetAddress.pollAny({ group: target }),
          unicast: Protocol.TargetAddress.pollUni({ group, id: pid }),

          anycast: (target) => Protocol.TargetAddress.pollAny({ group: target, id: Option.some(pid) }),
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

const isDebugSnapSuccess = Schema.is(Protocol.DebugSnapSuccessResponse);

export interface ResonateTestOptions {
  readonly group?: Protocol.WorkerGroup;
  readonly pid?: Protocol.ProcessId;
  readonly clientPid?: Protocol.ProcessId;
  readonly ttl?: Duration.Duration;
  readonly retryTimeout?: Duration.Duration;
  readonly tickInterval?: Duration.Duration;
}

export interface ResonateTestService {
  readonly snapshot: Effect.Effect<DebugState, unknown>;
  readonly restartWorker: Effect.Effect<void, unknown>;
}

export class ResonateTest extends Context.Service<ResonateTest, ResonateTestService>()(
  "effect-resonate/testing/ResonateTest",
) {
  static layer<const Fns extends ReadonlyArray<Resonate.AnyFunction>, E = never, R = never>(options: {
    readonly group: Resonate.FunctionGroup<Fns>;
    readonly handlers: Layer.Layer<Resonate.Handler<Fns[number]>, E, R>;
    readonly testOptions?: ResonateTestOptions;
  }) {
    const workerGroup = options.testOptions?.group ?? Protocol.WorkerGroup.make("default");
    const workerPid = options.testOptions?.pid ?? Protocol.ProcessId.make("worker-1");
    const ttl = options.testOptions?.ttl ?? Duration.seconds(30);
    const base = Layer.mergeAll(
      NetworkLocal.layer({
        group: workerGroup,
        pid: workerPid,
        tickInterval: options.testOptions?.tickInterval ?? Duration.seconds(1),
        retryTimeout: options.testOptions?.retryTimeout ?? Duration.seconds(5),
      }),
      BunCrypto.layer,
    );
    const core = Layer.mergeAll(DurablePromises.layer, Tasks.layer, Schedules.layer, options.handlers).pipe(
      Layer.provideMerge(base),
    );
    const services = Layer.mergeAll(
      Resonate.ResonateClient.layer({
        group: workerGroup,
        pid: options.testOptions?.clientPid ?? Protocol.ProcessId.make("client-1"),
        ttl,
      }),
    ).pipe(Layer.provideMerge(core));
    const worker = Worker.layer({ group: options.group, worker: { group: workerGroup, pid: workerPid, ttl } });
    const test = Layer.effect(
      ResonateTest,
      Effect.gen(function* () {
        const network = yield* ResonateNetwork;
        const context = yield* Effect.context<Crypto.Crypto | Resonate.Handler<Fns[number]> | ResonateNetwork>();
        const workerScope = yield* Ref.make<Option.Option<Scope.Closeable>>(Option.none());

        const startWorker = Effect.fn("ResonateTest.startWorker")(function* () {
          const scope = yield* Scope.make();
          yield* Layer.buildWithScope(worker, scope).pipe(Effect.provide(context));
          yield* Ref.set(workerScope, Option.some(scope));
        });

        const stopWorker = Effect.fn("ResonateTest.stopWorker")(function* () {
          const current = yield* Ref.get(workerScope);
          if (Option.isSome(current)) {
            yield* Scope.close(current.value, Exit.void).pipe(Effect.provide(context));
          }
          yield* Ref.set(workerScope, Option.none());
        });

        yield* startWorker();
        return ResonateTest.of({
          snapshot: Effect.gen(function* () {
            const response = yield* network.send(
              Protocol.DebugSnapRequest.make({
                head: Protocol.RequestHead.make({
                  corrId: Protocol.CorrelationId.make("resonate-test-snapshot"),
                  version: Protocol.protocolVersion,
                }),
                data: {},
              }),
            );
            if (!isDebugSnapSuccess(response)) {
              return yield* Effect.die(response.data);
            }
            return response.data;
          }),
          restartWorker: stopWorker().pipe(Effect.andThen(startWorker())),
        });
      }),
    );
    return test.pipe(Layer.provideMerge(services));
  }
}

export const snapshot: Effect.Effect<DebugState, unknown, ResonateTest> = ResonateTest.pipe(
  Effect.flatMap((test) => test.snapshot),
);

export const restartWorker: Effect.Effect<void, unknown, ResonateTest> = ResonateTest.pipe(
  Effect.flatMap((test) => test.restartWorker),
);
