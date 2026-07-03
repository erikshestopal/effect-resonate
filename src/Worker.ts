import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Crypto, Duration, Effect, Exit, HashMap, Layer, Predicate, Ref, SchemaParser, Stream } from "effect";
import { TaskFenced } from "./Errors.ts";
import * as NetworkHttp from "./NetworkHttp.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";
import { type AnyFunction, type FunctionGroup, type Handler } from "./Resonate.ts";
import { type ExecuteOptions, ExecutionEngine } from "./ResonateContext.ts";
import { SuspendAccepted, Tasks } from "./Task.ts";

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
      const registry = yield* group.registry();
      const pid = config.pid ?? Protocol.ProcessId.make(yield* Effect.orDie(crypto.randomUUIDv4));
      const ttl = config.ttl ?? Duration.seconds(60);
      const heartbeatEvery = Duration.millis(Math.max(1, Math.floor(Duration.toMillis(ttl) / 2)));
      const held = yield* Ref.make(HashMap.empty<Protocol.TaskId, HeldTask>());

      const addHeld = (task: HeldTask) => Ref.update(held, HashMap.set(task.id, task));
      const removeHeld = (id: Protocol.TaskId) => Ref.update(held, HashMap.remove(id));

      const heartbeat = Effect.gen(function* () {
        const current = yield* Ref.get(held);
        yield* tasks.heartbeat({ pid, tasks: HashMap.toValues(current) }).pipe(Effect.catchCause(() => Effect.void));
      }).pipe(Effect.delay(heartbeatEvery), Effect.forever);

      const suspendActions = (task: Protocol.TaskAcquired, awaited: ReadonlyArray<Protocol.PromiseId>) =>
        awaited.map((id) =>
          Protocol.PromiseRegisterCallbackRequest.make({
            head: Protocol.RequestHead.make({
              corrId: Protocol.CorrelationId.make(`${task.id}:${id}:callback`),
              version: Protocol.protocolVersion,
            }),
            data: { awaited: id, awaiter: task.id },
          }),
        );

      const executeUntilBlocked = Effect.fn("Worker.executeUntilBlocked")(function* (
        task: Protocol.TaskAcquired,
        promise: Protocol.PromiseRecord,
        registry: ExecuteOptions["registry"],
        preload: ReadonlyArray<Protocol.PromiseRecord>,
      ) {
        const currentPreload = yield* Ref.make(preload);
        yield* Effect.gen(function* () {
          const outcome = yield* engine.execute({
            task,
            promise,
            registry,
            preload: yield* Ref.get(currentPreload),
          });
          if (Predicate.isTagged(outcome, "Done")) {
            return false;
          }
          const result = yield* tasks.suspend(
            {
              id: task.id,
              version: task.version,
              actions: suspendActions(task, outcome.awaited),
            },
            {
              origin: promise.tags.reserved["resonate:origin"] ?? promise.id,
            },
          );
          if (SchemaParser.is(SuspendAccepted)(result)) {
            return false;
          }
          yield* Ref.set(currentPreload, result.preload);
          return true;
        }).pipe(Effect.repeat({ while: Predicate.isTruthy }));
      });

      const handleExecute = Effect.fn("Worker.handleExecute")(function* (message: Protocol.Message) {
        if (message.kind !== "execute") {
          return;
        }
        const acquired = yield* tasks
          .acquire(
            { id: message.data.task.id, version: message.data.task.version, pid, ttl },
            { origin: message.data.task.id },
          )
          .pipe(Effect.catchTag("TaskFenced", () => Effect.succeed(undefined)));
        if (Predicate.isUndefined(acquired)) {
          return;
        }
        if (acquired.task.state !== "acquired") {
          return;
        }
        yield* addHeld({ id: acquired.task.id, version: acquired.task.version });
        const exit = yield* executeUntilBlocked(acquired.task, acquired.promise, registry, acquired.preload).pipe(
          Effect.exit,
        );
        yield* removeHeld(acquired.task.id);
        if (Exit.isFailure(exit)) {
          yield* tasks
            .release(
              { id: acquired.task.id, version: acquired.task.version },
              { origin: acquired.promise.tags.reserved["resonate:origin"] ?? acquired.promise.id },
            )
            .pipe(
              Effect.catchTags({
                TaskFenced: (_error: TaskFenced) => Effect.void,
                PromiseNotFound: () => Effect.void,
                InvalidTarget: () => Effect.void,
                TransportError: () => Effect.void,
              }),
            );
        }
      });

      yield* heartbeat.pipe(Effect.forkScoped);
      yield* network.messages.pipe(
        Stream.runForEach((message) => handleExecute(message).pipe(Effect.catchCause(() => Effect.void))),
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
