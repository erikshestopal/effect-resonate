/**
 * Worker layers for executing registered Resonate function handlers.
 *
 * Workers subscribe to protocol messages from a {@link ResonateNetwork}, acquire
 * tasks, execute the matching handler through the execution engine, heartbeat
 * held tasks, and suspend tasks that are blocked on durable children.
 *
 * @example
 * ```ts
 * import { Duration, Layer } from "effect"
 * import * as BunCrypto from "@effect/platform-bun/BunCrypto"
 * import * as BunHttpClient from "@effect/platform-bun/BunHttpClient"
 * import { Worker } from "effect-resonate"
 *
 * const worker = Worker.layerHttp({
 *   group: App,
 *   http: { url: "http://127.0.0.1:8001", group: "default", ttl: Duration.seconds(30) }
 * }).pipe(
 *   Layer.provideMerge(handlers),
 *   Layer.provideMerge(BunHttpClient.layer),
 *   Layer.provideMerge(BunCrypto.layer)
 * )
 * ```
 *
 * @since 0.0.0
 */
import {
  Array as Arr,
  Cause,
  Context,
  Crypto,
  Duration,
  Effect,
  HashMap,
  Layer,
  Option,
  Predicate,
  Ref,
  Schedule,
  SchemaParser,
  Stream,
} from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import * as NetworkHttp from "./network/Http.ts";
import { ResonateNetwork } from "./network/Network.ts";
import * as Protocol from "./Protocol.ts";
import { type AnyFunction, type FunctionGroup, type Handler } from "./Resonate.ts";
import { ExecutionEngine } from "./ResonateContext.ts";
import type { Registry } from "./Registry.ts";
import { SuspendAccepted, type TaskClaimResult, Tasks } from "./Task.ts";

export interface WorkerConfig {
  readonly group: Protocol.WorkerGroup;
  readonly pid?: Protocol.ProcessId;
  readonly ttl?: Duration.Duration;
}

export interface HttpWorkerConfig extends WorkerConfig {
  readonly url: string;
  readonly token?: string;
}

interface HeldTask {
  readonly id: Protocol.TaskId;
  readonly version: Protocol.TaskVersion;
}

type WorkerRequirements<F extends AnyFunction> = ResonateNetwork | Crypto.Crypto | Handler<F>;

export interface WorkerRuntimeService {
  readonly runAcquired: (claim: {
    readonly task: Protocol.TaskAcquired;
    readonly promise: Protocol.PromiseRecord;
    readonly preload: ReadonlyArray<Protocol.PromiseRecord>;
  }) => Effect.Effect<void, unknown>;
}

export class WorkerRuntime extends Context.Service<WorkerRuntime, WorkerRuntimeService>()(
  "effect-resonate/WorkerRuntime",
) {
  static layer<R>(options: {
    readonly registry: Effect.Effect<Registry, never, R>;
    readonly pid: Protocol.ProcessId;
    readonly ttl: Duration.Duration;
  }): Layer.Layer<WorkerRuntime, never, R | Tasks | ExecutionEngine> {
    return Layer.effect(
      WorkerRuntime,
      Effect.gen(function* () {
        const tasks = yield* Tasks;
        const engine = yield* ExecutionEngine;
        const registry = yield* options.registry;
        const held = yield* Ref.make(HashMap.empty<Protocol.TaskId, HeldTask>());
        const heartbeatEvery = Duration.max(Duration.millis(1), Duration.divideUnsafe(options.ttl, 2));

        yield* Ref.get(held).pipe(
          Effect.flatMap((current) =>
            tasks.heartbeat(Protocol.TaskHeartbeatData.make({ pid: options.pid, tasks: HashMap.toValues(current) })),
          ),
          Effect.catchCause((cause) => Effect.logWarning("Worker heartbeat failed", cause)),
          Effect.schedule(Schedule.spaced(heartbeatEvery)),
          Effect.forkScoped,
        );

        const runAcquiredUntilBlocked = Effect.fn("WorkerRuntime.runAcquiredUntilBlocked")(function* (input: {
          readonly task: Protocol.TaskAcquired;
          readonly promise: Protocol.PromiseRecord;
          readonly preload: ReadonlyArray<Protocol.PromiseRecord>;
        }): Effect.fn.Return<void, unknown> {
          const { task, promise, preload } = input;
          const outcome = yield* engine.execute({ task, promise, registry, preload });
          if (Predicate.isTagged(outcome, "Done")) {
            return;
          }
          const result = yield* tasks.suspend({
            data: Protocol.TaskSuspendData.make({
              id: task.id,
              version: task.version,
              actions: Arr.map(outcome.awaited, (id) =>
                Protocol.PromiseRegisterCallbackRequest.make({
                  head: Protocol.RequestHead.make({
                    corrId: Protocol.CorrelationId.make(`${task.id}:${id}:callback`),
                    version: Protocol.protocolVersion,
                  }),
                  data: Protocol.PromiseRegisterCallbackData.make({ awaited: id, awaiter: task.id }),
                }),
              ),
            }),
            options: { origin: Protocol.promiseOrigin(promise) },
          });
          if (SchemaParser.is(SuspendAccepted)(result)) {
            return;
          }
          return yield* runAcquiredUntilBlocked({ task, promise, preload: result.preload });
        });

        return WorkerRuntime.of({
          runAcquired: Effect.fn("WorkerRuntime.runAcquired")(function* (claim) {
            yield* Ref.update(held, HashMap.set(claim.task.id, { id: claim.task.id, version: claim.task.version }));
            const releaseBestEffort = tasks
              .release({
                data: Protocol.TaskReleaseData.make({ id: claim.task.id, version: claim.task.version }),
                options: { origin: Protocol.promiseOrigin(claim.promise) },
              })
              .pipe(Effect.ignore);
            yield* runAcquiredUntilBlocked(claim).pipe(
              Effect.onError((cause) => (Cause.hasInterruptsOnly(cause) ? Effect.void : releaseBestEffort)),
              Effect.ensuring(Ref.update(held, HashMap.remove(claim.task.id))),
            );
          }),
        });
      }),
    );
  }
}

/**
 * Builds a worker layer against an already-provided network service.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = <const Fns extends ReadonlyArray<AnyFunction>>(config: {
  readonly group: FunctionGroup<Fns>;
  readonly worker: WorkerConfig;
}): Layer.Layer<WorkerRuntime, never, WorkerRequirements<Fns[number]>> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const pid = config.worker.pid ?? Protocol.ProcessId.make(yield* Effect.orDie(crypto.randomUUIDv4));
      const ttl = config.worker.ttl ?? Duration.seconds(60);

      return Layer.effectDiscard(
        Effect.gen(function* () {
          const network = yield* ResonateNetwork;
          const tasks = yield* Tasks;
          const runtime = yield* WorkerRuntime;
          const handleExecute = Effect.fn("Worker.handleExecute")(function* (message: Protocol.ExecuteMessage) {
            const acquired = yield* tasks
              .acquire({
                data: Protocol.TaskAcquireData.make({
                  id: message.data.task.id,
                  version: message.data.task.version,
                  pid,
                  ttl,
                }),
                options: { origin: message.data.task.id },
              })
              .pipe(
                Effect.map(Option.some),
                Effect.catchTag("TaskFenced", () => Effect.succeed(Option.none<TaskClaimResult>())),
                Effect.map(
                  Option.filter((claim): claim is TaskClaimResult & { readonly task: Protocol.TaskAcquired } =>
                    SchemaParser.is(Protocol.TaskAcquired)(claim.task),
                  ),
                ),
              );
            if (Option.isNone(acquired)) {
              return;
            }
            yield* runtime.runAcquired(acquired.value);
          });

          yield* network.messages.pipe(
            Stream.filter(Protocol.Message.guards.execute),
            Stream.runForEach((message) =>
              handleExecute(message).pipe(
                Effect.catchCause((cause) => Effect.logWarning("Worker handleExecute failed", cause)),
              ),
            ),
            Effect.forkScoped,
          );
        }),
      ).pipe(
        Layer.provideMerge(WorkerRuntime.layer({ registry: config.group.registry, pid, ttl })),
        Layer.provideMerge(ExecutionEngine.layer.pipe(Layer.provideMerge(Tasks.layer))),
        Layer.provideMerge(Tasks.layer),
      );
    }),
  );

/**
 * Builds a worker layer backed by the HTTP network implementation.
 *
 * This layer stays runtime-neutral: callers provide the concrete Effect
 * `HttpClient` and `Crypto` implementations for Bun, Node, or another runtime.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerHttp = <const Fns extends ReadonlyArray<AnyFunction>>(config: {
  readonly group: FunctionGroup<Fns>;
  readonly http: HttpWorkerConfig;
}): Layer.Layer<WorkerRuntime, never, Handler<Fns[number]> | HttpClient.HttpClient | Crypto.Crypto> =>
  layer({ group: config.group, worker: config.http }).pipe(
    Layer.provideMerge(
      NetworkHttp.layer({
        url: config.http.url,
        group: config.http.group,
        pid: config.http.pid,
        token: config.http.token,
      }),
    ),
  );
