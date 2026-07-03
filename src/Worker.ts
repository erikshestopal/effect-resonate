import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import {
  Cause,
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
import * as NetworkHttp from "./network/http.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";
import { type AnyFunction, type FunctionGroup, type Handler } from "./Resonate.ts";
import { type ExecuteOptions, ExecutionEngine } from "./ResonateContext.ts";
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

export const layer = <const Fns extends ReadonlyArray<AnyFunction>>(
  group: FunctionGroup<Fns>,
  config: WorkerConfig,
): Layer.Layer<never, never, WorkerRequirements<Fns[number]>> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const tasks = yield* Tasks;
      const engine = yield* ExecutionEngine;
      const crypto = yield* Crypto.Crypto;
      const registry = yield* group.registry;
      const pid = config.pid ?? Protocol.ProcessId.make(yield* Effect.orDie(crypto.randomUUIDv4));
      const ttl = config.ttl ?? Duration.seconds(60);
      const heartbeatEvery = Duration.max(Duration.millis(1), Duration.divideUnsafe(ttl, 2));
      const held = yield* Ref.make(HashMap.empty<Protocol.TaskId, HeldTask>());

      const heartbeat = Ref.get(held).pipe(
        Effect.flatMap((current) => tasks.heartbeat({ pid, tasks: HashMap.toValues(current) })),
        Effect.catchCause((cause) => Effect.logWarning("Worker heartbeat failed", cause)),
        Effect.schedule(Schedule.spaced(heartbeatEvery)),
      );

      const executeUntilBlocked = Effect.fn("Worker.executeUntilBlocked")(function* (
        task: Protocol.TaskAcquired,
        promise: Protocol.PromiseRecord,
        registry: ExecuteOptions["registry"],
        preload: ReadonlyArray<Protocol.PromiseRecord>,
      ): Effect.fn.Return<void, unknown> {
        const outcome = yield* engine.execute({ task, promise, registry, preload });
        if (Predicate.isTagged(outcome, "Done")) {
          return;
        }
        const result = yield* tasks.suspend(
          {
            id: task.id,
            version: task.version,
            actions: outcome.awaited.map((id) =>
              Protocol.PromiseRegisterCallbackRequest.make({
                head: Protocol.RequestHead.make({
                  corrId: Protocol.CorrelationId.make(`${task.id}:${id}:callback`),
                  version: Protocol.protocolVersion,
                }),
                data: { awaited: id, awaiter: task.id },
              }),
            ),
          },
          {
            origin: Protocol.promiseOrigin(promise),
          },
        );
        if (SchemaParser.is(SuspendAccepted)(result)) {
          return;
        }
        return yield* executeUntilBlocked(task, promise, registry, result.preload);
      });

      const handleExecute = Effect.fn("Worker.handleExecute")(function* (message: Protocol.ExecuteMessage) {
        const acquired = yield* tasks
          .acquire(
            { id: message.data.task.id, version: message.data.task.version, pid, ttl },
            { origin: message.data.task.id },
          )
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
        const claim = acquired.value;
        yield* Ref.update(held, HashMap.set(claim.task.id, { id: claim.task.id, version: claim.task.version }));
        const releaseBestEffort = tasks
          .release(
            { id: claim.task.id, version: claim.task.version },
            { origin: Protocol.promiseOrigin(claim.promise) },
          )
          .pipe(Effect.ignore);
        yield* executeUntilBlocked(claim.task, claim.promise, registry, claim.preload).pipe(
          Effect.onError((cause) => (Cause.hasInterruptsOnly(cause) ? Effect.void : releaseBestEffort)),
          Effect.ensuring(Ref.update(held, HashMap.remove(claim.task.id))),
        );
      });

      yield* heartbeat.pipe(Effect.forkScoped);
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
  ).pipe(Layer.provideMerge(ExecutionEngine.layer.pipe(Layer.provideMerge(Tasks.layer))));

export const layerHttp = <const Fns extends ReadonlyArray<AnyFunction>>(
  group: FunctionGroup<Fns>,
  config: HttpWorkerConfig,
): Layer.Layer<never, never, Handler<Fns[number]>> =>
  layer(group, config).pipe(
    Layer.provideMerge(
      NetworkHttp.layer({
        url: config.url,
        group: config.group,
        pid: config.pid,
        token: config.token,
      }).pipe(Layer.provide(BunHttpClient.layer)),
    ),
    Layer.provideMerge(BunCrypto.layer),
  );
