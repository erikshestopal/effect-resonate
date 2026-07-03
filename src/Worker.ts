import { Crypto, Duration, Effect, Exit, Layer, Predicate, Ref, SchemaParser, Stream } from "effect";
import { TaskFenced } from "./Errors.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";
import { type AnyFunction, type FunctionGroup } from "./Resonate.ts";
import { ExecutionEngine } from "./ResonateContext.ts";
import { SuspendAccepted, Tasks } from "./Task.ts";

export interface WorkerConfig {
  readonly group: Protocol.WorkerGroup;
  readonly pid?: Protocol.ProcessId;
  readonly ttl?: Duration.Duration;
}

interface HeldTask {
  readonly id: Protocol.TaskId;
  readonly version: Protocol.TaskVersion;
}

export const layer = <const Fns extends ReadonlyArray<AnyFunction>>(
  group: FunctionGroup<Fns>,
  config: WorkerConfig,
): Layer.Layer<
  never,
  never,
  ResonateNetwork | Tasks | ExecutionEngine | Crypto.Crypto | import("./Resonate.ts").Handler<Fns[number]>
> =>
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
      const held = yield* Ref.make(new Map<string, HeldTask>());

      const addHeld = (task: HeldTask) => Ref.update(held, (current) => new Map(current).set(task.id, task));
      const removeHeld = (id: Protocol.TaskId) =>
        Ref.update(held, (current) => {
          const next = new Map(current);
          next.delete(id);
          return next;
        });

      const heartbeat = Effect.gen(function* () {
        const current = yield* Ref.get(held);
        yield* tasks.heartbeat({ pid, tasks: [...current.values()] }).pipe(Effect.catchCause(() => Effect.void));
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
        registry: import("./ResonateContext.ts").ExecuteOptions["registry"],
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
          const result = yield* tasks.suspend({
            id: task.id,
            version: task.version,
            actions: suspendActions(task, outcome.awaited),
          });
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
          .acquire({ id: message.data.task.id, version: message.data.task.version, pid, ttl })
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
          yield* tasks.release({ id: acquired.task.id, version: acquired.task.version }).pipe(
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
  );
