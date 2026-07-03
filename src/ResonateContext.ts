import {
  Clock,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Predicate,
  Result,
  Schema,
  SchemaParser,
} from "effect";
import { ResonateCodec } from "./Codec.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut } from "./Errors.ts";
import * as Protocol from "./Protocol.ts";
import type { Registry } from "./Resonate.ts";
import { Tasks } from "./Task.ts";

export class DurablePanic extends Schema.TaggedErrorClass<DurablePanic>()("DurablePanic", {
  message: Schema.String,
}) {}

class SuspendedExecution extends Schema.TaggedErrorClass<SuspendedExecution>()("SuspendedExecution", {
  awaited: Schema.Array(Protocol.PromiseId),
}) {}

const isSuspendedExecution = SchemaParser.is(SuspendedExecution);

const InvocationParam = Schema.Struct({
  func: Schema.String,
  args: Schema.Array(Schema.Unknown),
  version: Protocol.FunctionVersionFromWire,
});

export interface ContextOptions {
  readonly id?: Protocol.PromiseId;
  readonly timeout?: Duration.Duration;
  readonly tags?: Protocol.Tags;
}

export interface ContextInfo {
  readonly id: Protocol.PromiseId;
  readonly originId: Protocol.PromiseId;
  readonly prefixId: Protocol.PromiseId;
  readonly parentId: Protocol.PromiseId;
  readonly branchId: Protocol.PromiseId;
  readonly timeoutAt: Protocol.Timestamp;
  readonly version: Protocol.TaskVersion;
}

export interface LocalDurableHandle {
  readonly id: Protocol.PromiseId;
  readonly await: Effect.Effect<unknown, unknown>;
  readonly poll: Effect.Effect<Option.Option<Exit.Exit<unknown, unknown>>, unknown>;
  readonly cancel: Effect.Effect<void, unknown>;
}

interface RunningChild {
  readonly id: Protocol.PromiseId;
  readonly fiber: Fiber.Fiber<unknown, unknown>;
}

interface RuntimeState {
  readonly root: Protocol.PromiseId;
  readonly version: Protocol.TaskVersion;
  readonly timeoutAt: Protocol.Timestamp;
  readonly originId: Protocol.PromiseId;
  readonly prefixId: Protocol.PromiseId;
  readonly parentId: Protocol.PromiseId;
  readonly branchId: Protocol.PromiseId;
  readonly cache: Map<Protocol.PromiseId, Protocol.PromiseRecord>;
  readonly children: Array<RunningChild>;
  readonly awaiting: Map<Protocol.PromiseId, SuspendedExecution>;
  seq: number;
}

export class EngineDone extends Schema.Class<EngineDone>("ExecutionEngine/Done")({
  _tag: Schema.tag("Done"),
  promise: Protocol.PromiseRecord,
}) {}

export class EngineSuspended extends Schema.Class<EngineSuspended>("ExecutionEngine/Suspended")({
  _tag: Schema.tag("Suspended"),
  awaited: Schema.Array(Protocol.PromiseId),
}) {}

class CompletedExecution extends Schema.Class<CompletedExecution>("ExecutionEngine/Completed")({
  _tag: Schema.tag("Completed"),
  value: Schema.Unknown,
}) {}

export type EngineOutcome = EngineDone | EngineSuspended;

export interface ExecuteOptions {
  readonly task: Protocol.TaskAcquired;
  readonly promise: Protocol.PromiseRecord;
  readonly registry: Registry;
  readonly preload?: ReadonlyArray<Protocol.PromiseRecord>;
}

const localScope = Schema.Literal("local").make("local");
const resolvedState = Schema.Literal("resolved").make("resolved");
const rejectedState = Schema.Literal("rejected").make("rejected");

const timestamp = (millis: number): Protocol.Timestamp => Schema.decodeUnknownSync(Protocol.Timestamp)(millis);

const childId = (parent: Protocol.PromiseId, seq: number): Protocol.PromiseId =>
  Protocol.PromiseId.make(`${parent}.${seq}`);

const requestHead = (corrId: string): Protocol.RequestHead =>
  Protocol.RequestHead.make({
    corrId: Protocol.CorrelationId.make(corrId),
    version: Protocol.protocolVersion,
  });

const doneOutcome = (promise: Protocol.PromiseRecord): EngineOutcome => new EngineDone({ promise });

const suspendedOutcome = (awaited: ReadonlyArray<Protocol.PromiseId>): EngineOutcome =>
  new EngineSuspended({ awaited });

const isExternalPromise = (promise: Protocol.PromiseRecord): boolean =>
  Predicate.isNotUndefined(promise.tags.reserved["resonate:target"]) ||
  Predicate.isNotUndefined(promise.tags.reserved["resonate:timer"]);

const isPromiseCreateSuccess = SchemaParser.is(Protocol.PromiseCreateResponse.members[0]);
const isPromiseSettleSuccess = SchemaParser.is(Protocol.PromiseSettleResponse.members[0]);

export interface ResonateContextService {
  readonly info: ContextInfo;
  readonly run: (effect: Effect.Effect<unknown, unknown>, options?: ContextOptions) => Effect.Effect<unknown, unknown>;
  readonly beginRun: (
    effect: Effect.Effect<unknown, unknown>,
    options?: ContextOptions,
  ) => Effect.Effect<LocalDurableHandle, unknown>;
  readonly all: <const Effects extends ReadonlyArray<Effect.Effect<unknown, unknown, unknown>>>(
    effects: Effects,
  ) => Effect.Effect<ReadonlyArray<unknown>, unknown, unknown>;
  readonly panic: (message: string) => Effect.Effect<never, DurablePanic>;
}

export class ResonateContext extends Context.Service<ResonateContext, ResonateContextService>()(
  "effect-resonate/Context",
) {}

export interface ExecutionEngineService {
  readonly execute: (options: ExecuteOptions) => Effect.Effect<EngineOutcome, unknown>;
}

export class ExecutionEngine extends Context.Service<ExecutionEngine, ExecutionEngineService>()(
  "effect-resonate/ExecutionEngine",
) {
  static readonly layer: Layer.Layer<ExecutionEngine, never, Tasks | ResonateCodec> = Layer.effect(
    ExecutionEngine,
    Effect.gen(function* () {
      const tasks = yield* Tasks;
      const codec = yield* ResonateCodec;

      const addPreload = (state: RuntimeState, preload: ReadonlyArray<Protocol.PromiseRecord>) => {
        for (const promise of preload) {
          state.cache.set(promise.id, promise);
        }
      };

      const decodeSettled = Effect.fn("ExecutionEngine.decodeSettled")(function* (promise: Protocol.PromiseRecord) {
        if (promise.state === "pending") {
          return yield* Effect.die(`Promise '${promise.id}' is still pending`);
        }
        if (promise.state === "rejected_canceled") {
          return yield* new DurablePromiseCanceled({ id: promise.id });
        }
        if (promise.state === "rejected_timedout") {
          return yield* new DurablePromiseTimedOut({ id: promise.id });
        }
        const value = yield* codec.decode(promise.value);
        if (promise.state === "resolved") {
          return value;
        }
        return yield* Effect.fail(value);
      });

      const actionPromise = (action: unknown): Effect.Effect<Protocol.PromiseRecord> => {
        if (isPromiseCreateSuccess(action) || isPromiseSettleSuccess(action)) {
          return Effect.succeed(action.data.promise);
        }
        return Effect.die(action);
      };

      const settle = Effect.fn("ExecutionEngine.settle")(function* (
        state: RuntimeState,
        id: Protocol.PromiseId,
        exit: Exit.Exit<unknown, unknown>,
      ) {
        const settled = Exit.isSuccess(exit) ? resolvedState : rejectedState;
        const value = yield* codec.encode(Exit.isSuccess(exit) ? exit.value : exit.cause);
        const result = yield* tasks.fence({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseSettleRequest.make({
            head: requestHead(`${state.root}:${id}:settle`),
            data: { id, state: settled, value },
          }),
        });
        addPreload(state, result.preload);
        const promise = yield* actionPromise(result.action);
        state.cache.set(promise.id, promise);
        return promise;
      });

      const fulfillRoot = Effect.fn("ExecutionEngine.fulfillRoot")(function* (
        state: RuntimeState,
        exit: Exit.Exit<unknown, unknown>,
      ) {
        const settled = Exit.isSuccess(exit) ? resolvedState : rejectedState;
        const value = yield* codec.encode(Exit.isSuccess(exit) ? exit.value : exit.cause);
        const promise = yield* tasks.fulfill({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseSettleRequest.make({
            head: requestHead(`${state.root}:fulfill`),
            data: { id: state.root, state: settled, value },
          }),
        });
        state.cache.set(promise.id, promise);
        return promise;
      });

      const timeoutAt = Effect.fn("ExecutionEngine.timeoutAt")(function* (
        parent: Protocol.Timestamp,
        duration: Duration.Duration,
      ) {
        const now = yield* Clock.currentTimeMillis;
        return timestamp(Math.min(now + Duration.toMillis(duration), DateTime.toEpochMillis(parent)));
      });

      const localTags = (state: RuntimeState, id: Protocol.PromiseId, extra: Protocol.Tags): Protocol.Tags =>
        Protocol.Tags.make({
          reserved: {
            ...extra.reserved,
            "resonate:origin": state.originId,
            "resonate:prefix": state.prefixId,
            "resonate:branch": state.branchId,
            "resonate:parent": state.root,
            "resonate:scope": localScope,
          },
          unrecognized: extra.unrecognized,
          user: extra.user,
        });

      const createLocal = Effect.fn("ExecutionEngine.createLocal")(function* (
        state: RuntimeState,
        options: ContextOptions | undefined,
      ) {
        const id = options?.id ?? childId(state.root, state.seq);
        state.seq = state.seq + 1;
        const cached = state.cache.get(id);
        if (Predicate.isNotUndefined(cached)) {
          return cached;
        }
        const result = yield* tasks.fence({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseCreateRequest.make({
            head: requestHead(`${state.root}:${id}:create`),
            data: {
              id,
              timeoutAt: yield* timeoutAt(state.timeoutAt, options?.timeout ?? Duration.hours(24)),
              param: Protocol.emptyValue,
              tags: localTags(state, id, options?.tags ?? Protocol.emptyTags),
            },
          }),
        });
        addPreload(state, result.preload);
        const promise = yield* actionPromise(result.action);
        state.cache.set(promise.id, promise);
        return promise;
      });

      const makeHandle = (
        state: RuntimeState,
        promise: Protocol.PromiseRecord,
        deferred: Deferred.Deferred<unknown, unknown>,
      ): LocalDurableHandle => ({
        id: promise.id,
        await:
          isExternalPromise(promise) && promise.state === "pending"
            ? Effect.suspend(() => {
                const parked = state.awaiting.get(promise.id);
                if (Predicate.isNotUndefined(parked)) {
                  return Effect.fail(parked);
                }
                const suspended = new SuspendedExecution({ awaited: [promise.id] });
                state.awaiting.set(promise.id, suspended);
                return Effect.fail(suspended);
              })
            : Deferred.await(deferred),
        poll: Deferred.poll(deferred).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.succeed(Option.none()),
              onSome: (awaited) => awaited.pipe(Effect.exit, Effect.map(Option.some)),
            }),
          ),
        ),
        cancel: settle(state, promise.id, Exit.fail(new DurablePromiseCanceled({ id: promise.id }))).pipe(
          Effect.asVoid,
        ),
      });

      const drainChildren = Effect.fn("ExecutionEngine.drainChildren")(function* (state: RuntimeState) {
        for (const child of state.children) {
          yield* Fiber.join(child.fiber).pipe(Effect.exit);
        }
      });

      const layerFor = (state: RuntimeState): Layer.Layer<ResonateContext, never, never> => {
        const beginRun: ResonateContextService["beginRun"] = Effect.fn("ResonateContext.beginRun")(function* (
          effect: Effect.Effect<unknown, unknown>,
          options?: ContextOptions,
        ): Effect.fn.Return<LocalDurableHandle, unknown> {
          const promise = yield* createLocal(state, options);
          const deferred = yield* Deferred.make<unknown, unknown>();
          if (promise.state !== "pending") {
            const settled = yield* decodeSettled(promise).pipe(Effect.exit);
            yield* Deferred.done(deferred, settled);
            return makeHandle(state, promise, deferred);
          }
          if (isExternalPromise(promise)) {
            return makeHandle(state, promise, deferred);
          }
          const fiber = yield* effect.pipe(
            Effect.exit,
            Effect.flatMap((exit) => settle(state, promise.id, exit)),
            Effect.flatMap((settled) =>
              decodeSettled(settled).pipe(
                Effect.exit,
                Effect.flatMap((exit) => Deferred.done(deferred, exit)),
              ),
            ),
            Effect.forkDetach,
          );
          state.children.push({ id: promise.id, fiber });
          return makeHandle(state, promise, deferred);
        });

        const run: ResonateContextService["run"] = Effect.fn("ResonateContext.run")(function* (effect, options) {
          const handle = yield* beginRun(effect, options);
          return yield* handle.await;
        });

        return Layer.succeed(
          ResonateContext,
          ResonateContext.of({
            info: {
              id: state.root,
              originId: state.originId,
              prefixId: state.prefixId,
              parentId: state.parentId,
              branchId: state.branchId,
              timeoutAt: state.timeoutAt,
              version: state.version,
            },
            run,
            beginRun,
            all: (effects) =>
              Effect.gen(function* () {
                const exits = yield* Effect.forEach(effects, (effect) => effect.pipe(Effect.result));
                const values: Array<unknown> = [];
                const awaited: Array<Protocol.PromiseId> = [];
                for (const exit of exits) {
                  if (Result.isSuccess(exit)) {
                    values.push(exit.success);
                    continue;
                  }
                  if (isSuspendedExecution(exit.failure)) {
                    awaited.push(...exit.failure.awaited);
                    continue;
                  }
                  return yield* Effect.fail(exit.failure);
                }
                if (awaited.length > 0) {
                  return yield* new SuspendedExecution({ awaited });
                }
                return values;
              }),
            panic: (message) => new DurablePanic({ message }),
          }),
        );
      };

      const execute: ExecutionEngineService["execute"] = Effect.fn("ExecutionEngine.execute")(function* (options) {
        const state: RuntimeState = {
          root: options.promise.id,
          version: options.task.version,
          timeoutAt: options.promise.timeoutAt,
          originId: options.promise.tags.reserved["resonate:origin"] ?? options.promise.id,
          prefixId: options.promise.tags.reserved["resonate:prefix"] ?? options.promise.id,
          parentId: options.promise.tags.reserved["resonate:parent"] ?? options.promise.id,
          branchId: options.promise.tags.reserved["resonate:branch"] ?? options.promise.id,
          cache: new Map(),
          children: [],
          awaiting: new Map(),
          seq: 0,
        };
        addPreload(state, options.preload ?? []);

        if (options.promise.state !== "pending") {
          return doneOutcome(options.promise);
        }

        const invocation = yield* codec
          .decode(options.promise.param)
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(InvocationParam)));
        const item = yield* Option.match(options.registry.get(invocation.func, invocation.version), {
          onNone: () => Effect.die(`Function '${invocation.func}' is not registered`),
          onSome: Effect.succeed,
        });
        const decoded = yield* Schema.decodeUnknownEffect(item.definition.payload)(invocation.args).pipe(
          Effect.catchCause(() => Schema.decodeUnknownEffect(item.definition.payload)(invocation.args[0])),
        );
        const args = Array.isArray(decoded) ? decoded : [decoded];
        const result: Effect.Effect<unknown, unknown> = Reflect.apply(item.handler, undefined, args);
        if (!Effect.isEffect(result)) {
          return yield* Effect.die(`Function '${invocation.func}' did not return an Effect`);
        }
        const exit = yield* result.pipe(
          Effect.provide(layerFor(state)),
          Effect.map((value) => new CompletedExecution({ value })),
          Effect.catch((error) => (isSuspendedExecution(error) ? Effect.succeed(error) : Effect.fail(error))),
          Effect.exit,
        );
        yield* drainChildren(state);
        if (Exit.isSuccess(exit)) {
          if (Predicate.isTagged(exit.value, "SuspendedExecution")) {
            return suspendedOutcome([...new Set(exit.value.awaited)]);
          }
          const promise = yield* fulfillRoot(state, Exit.succeed(exit.value.value));
          return doneOutcome(promise);
        }
        const promise = yield* fulfillRoot(state, exit);
        return doneOutcome(promise);
      });

      return ExecutionEngine.of({ execute });
    }),
  );
}
