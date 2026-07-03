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
import { ResonateCodec, withSchemaHeader } from "./Codec.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, EncodingError } from "./Errors.ts";
import * as Protocol from "./Protocol.ts";
import type { AnyFunction, PayloadArgs, PromiseDeclaration, PromiseSuccess, Registry } from "./Resonate.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
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
  retry: Schema.optionalKey(RetryPolicy.RetryPolicyFromWire),
});

export interface ContextOptions {
  readonly id?: Protocol.PromiseId;
  readonly target?: Protocol.WorkerGroup;
  readonly timeout?: Duration.Duration;
  readonly tags?: Protocol.Tags;
  readonly version?: Protocol.FunctionVersionOrLatest;
  readonly retryPolicy?: RetryPolicy.RetryPolicy;
  readonly nonRetryableErrors?: ReadonlyArray<Schema.Codec<unknown, unknown, never, never>>;
}

export interface ContextInfo {
  readonly attempt: number;
  readonly id: Protocol.PromiseId;
  readonly originId: Protocol.PromiseId;
  readonly prefixId: Protocol.PromiseId;
  readonly parentId: Protocol.PromiseId;
  readonly branchId: Protocol.PromiseId;
  readonly timeoutAt: Protocol.Timestamp;
  readonly version: Protocol.TaskVersion;
}

export interface LocalDurableHandle<A = unknown, E = unknown> {
  readonly id: Protocol.PromiseId;
  readonly await: Effect.Effect<A, E>;
  readonly poll: Effect.Effect<Option.Option<Exit.Exit<A, E>>, unknown>;
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
  readonly targetTransport: "poll" | "local";
  readonly targetGroup: Protocol.WorkerGroup;
  readonly originId: Protocol.PromiseId;
  readonly prefixId: Protocol.PromiseId;
  readonly parentId: Protocol.PromiseId;
  readonly branchId: Protocol.PromiseId;
  readonly cache: Map<Protocol.PromiseId, Protocol.PromiseRecord>;
  readonly children: Array<RunningChild>;
  readonly attachedRemote: Map<Protocol.PromiseId, SuspendedExecution>;
  readonly awaiting: Map<Protocol.PromiseId, SuspendedExecution>;
  readonly externalPromises: Set<string>;
  attempt: number;
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
const globalScope = Schema.Literal("global").make("global");
const timerTag = Schema.Literal("true").make("true");
const resolvedState = Schema.Literal("resolved").make("resolved");
const rejectedState = Schema.Literal("rejected").make("rejected");

const timestamp = (millis: number): Protocol.Timestamp => Schema.decodeUnknownSync(Protocol.Timestamp)(millis);

const childId = (parent: Protocol.PromiseId, seq: number): Protocol.PromiseId =>
  Protocol.PromiseId.make(`${parent}.${seq}`);

const detachedId = (prefix: Protocol.PromiseId, seqid: Protocol.PromiseId): Protocol.PromiseId => {
  const bytes = new TextEncoder().encode(seqid);
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;
  for (const byte of bytes) {
    h1 = Math.imul(h1 ^ byte, 2654435761);
    h2 = Math.imul(h2 ^ byte, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  const hash = (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(16).padStart(14, "0");
  return Protocol.PromiseId.make(`${prefix}.d${hash}`);
};

const requestHead = (corrId: string): Protocol.RequestHead =>
  Protocol.RequestHead.make({
    corrId: Protocol.CorrelationId.make(corrId),
    version: Protocol.protocolVersion,
  });

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
  readonly all: <const Effects extends ReadonlyArray<Effect.Effect<unknown, unknown>>>(
    effects: Effects,
  ) => Effect.Effect<ReadonlyArray<unknown>, unknown>;
  readonly sleep: (duration: Duration.Input) => Effect.Effect<void, unknown>;
  readonly sleepUntil: (instant: DateTime.Utc) => Effect.Effect<void, unknown>;
  readonly beginRpc: {
    <F extends AnyFunction>(
      fn: F,
      args: PayloadArgs<F>,
      options?: ContextOptions,
    ): Effect.Effect<LocalDurableHandle, unknown>;
    (name: string, args: ReadonlyArray<unknown>, options?: ContextOptions): Effect.Effect<LocalDurableHandle, unknown>;
  };
  readonly rpc: {
    <F extends AnyFunction>(fn: F, args: PayloadArgs<F>, options?: ContextOptions): Effect.Effect<unknown, unknown>;
    (name: string, args: ReadonlyArray<unknown>, options?: ContextOptions): Effect.Effect<unknown, unknown>;
  };
  readonly detached: {
    <F extends AnyFunction>(
      fn: F,
      args: PayloadArgs<F>,
      options?: ContextOptions,
    ): Effect.Effect<LocalDurableHandle, unknown>;
    (name: string, args: ReadonlyArray<unknown>, options?: ContextOptions): Effect.Effect<LocalDurableHandle, unknown>;
  };
  readonly promise: <P extends PromiseDeclaration>(
    declaration: P,
    options?: Pick<ContextOptions, "id" | "timeout" | "tags">,
  ) => Effect.Effect<LocalDurableHandle<PromiseSuccess<P>, unknown>, unknown>;
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

      const encodeInvocation = Effect.fn("ExecutionEngine.encodeInvocation")(function* (
        name: string,
        args: ReadonlyArray<unknown>,
        version: Protocol.FunctionVersionOrLatest,
        retry?: RetryPolicy.RetryPolicy,
      ): Effect.fn.Return<Protocol.Value, unknown> {
        const encoded = yield* codec.encode(
          InvocationParam.make({
            func: name,
            args,
            version,
            ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
          }),
        );
        return withSchemaHeader(encoded, name);
      });

      const encodeTargetPayload = Effect.fn("ExecutionEngine.encodeTargetPayload")(function* (
        target: AnyFunction | string,
        args: ReadonlyArray<unknown>,
        options?: ContextOptions,
      ): Effect.fn.Return<Protocol.Value, unknown> {
        if (Predicate.isString(target)) {
          return yield* encodeInvocation(
            target,
            args,
            Predicate.isUndefined(options?.version) ? Protocol.FunctionVersion.make(1) : options.version,
            options?.retryPolicy,
          );
        }
        const encodedArgs = yield* Schema.encodeUnknownEffect(target.payload)(args).pipe(
          Effect.catchCause(() =>
            args.length === 1
              ? Schema.encodeUnknownEffect(target.payload)(args[0])
              : Effect.die("Invalid function payload"),
          ),
        );
        return yield* encodeInvocation(
          target.name,
          Array.isArray(encodedArgs) ? encodedArgs : [encodedArgs],
          options?.version ?? target.version,
          options?.retryPolicy,
        );
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

      const createSleep = Effect.fn("ExecutionEngine.createSleep")(function* (
        state: RuntimeState,
        instant: DateTime.Utc,
      ) {
        const id = childId(state.root, state.seq);
        state.seq = state.seq + 1;
        const cached = state.cache.get(id);
        if (Predicate.isNotUndefined(cached)) {
          return cached;
        }
        const result = yield* tasks.fence({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseCreateRequest.make({
            head: requestHead(`${state.root}:${id}:sleep`),
            data: {
              id,
              timeoutAt: timestamp(Math.min(DateTime.toEpochMillis(instant), DateTime.toEpochMillis(state.timeoutAt))),
              param: Protocol.emptyValue,
              tags: Protocol.Tags.make({
                reserved: {
                  "resonate:origin": state.originId,
                  "resonate:prefix": state.prefixId,
                  "resonate:branch": id,
                  "resonate:parent": state.root,
                  "resonate:scope": globalScope,
                  "resonate:timer": timerTag,
                },
                unrecognized: {},
                user: {},
              }),
            },
          }),
        });
        addPreload(state, result.preload);
        const promise = yield* actionPromise(result.action);
        state.cache.set(promise.id, promise);
        return promise;
      });

      const createExternalPromise = Effect.fn("ExecutionEngine.createExternalPromise")(function* (
        state: RuntimeState,
        declaration: PromiseDeclaration,
        options: Pick<ContextOptions, "id" | "timeout" | "tags"> | undefined,
      ) {
        if (Predicate.isUndefined(options?.id)) {
          if (state.externalPromises.has(declaration.name)) {
            return yield* Effect.die(`Promise declaration '${declaration.name}' was created more than once`);
          }
          state.externalPromises.add(declaration.name);
        }
        const id = options?.id ?? declaration.id(state.root);
        const cached = state.cache.get(id);
        if (Predicate.isNotUndefined(cached)) {
          if (cached.state === "pending") {
            state.attachedRemote.set(cached.id, new SuspendedExecution({ awaited: [cached.id] }));
          }
          return cached;
        }
        const result = yield* tasks.fence({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseCreateRequest.make({
            head: requestHead(`${state.root}:${id}:promise`),
            data: {
              id,
              timeoutAt: yield* timeoutAt(state.timeoutAt, options?.timeout ?? Duration.hours(24)),
              param: Protocol.emptyValue,
              tags: Protocol.Tags.make({
                reserved: {
                  ...(options?.tags ?? Protocol.emptyTags).reserved,
                  "resonate:origin": state.originId,
                  "resonate:prefix": state.prefixId,
                  "resonate:branch": id,
                  "resonate:parent": state.root,
                  "resonate:scope": globalScope,
                },
                unrecognized: options?.tags?.unrecognized ?? {},
                user: options?.tags?.user ?? {},
              }),
            },
          }),
        });
        addPreload(state, result.preload);
        const promise = yield* actionPromise(result.action);
        state.cache.set(promise.id, promise);
        if (promise.state === "pending") {
          state.attachedRemote.set(promise.id, new SuspendedExecution({ awaited: [promise.id] }));
        }
        return promise;
      });

      const createRemote = Effect.fn("ExecutionEngine.createRemote")(function* (
        state: RuntimeState,
        targetFunction: AnyFunction | string,
        args: ReadonlyArray<unknown>,
        options: ContextOptions | undefined,
        mode: "attached" | "detached",
      ) {
        const seqid = childId(state.root, state.seq);
        const id = options?.id ?? (mode === "detached" ? detachedId(state.prefixId, seqid) : seqid);
        state.seq = state.seq + 1;
        const cached = state.cache.get(id);
        if (Predicate.isNotUndefined(cached)) {
          if (mode === "attached" && cached.state === "pending") {
            state.attachedRemote.set(cached.id, new SuspendedExecution({ awaited: [cached.id] }));
          }
          return cached;
        }
        const targetGroup = options?.target ?? state.targetGroup;
        const target =
          state.targetTransport === "local"
            ? Protocol.TargetAddress.localAny(targetGroup)
            : Protocol.TargetAddress.pollAny(targetGroup);
        const now = yield* Clock.currentTimeMillis;
        const result = yield* tasks.fence({
          id: state.root,
          version: state.version,
          action: Protocol.PromiseCreateRequest.make({
            head: requestHead(`${state.root}:${id}:rpc`),
            data: {
              id,
              timeoutAt:
                mode === "detached"
                  ? timestamp(now + Duration.toMillis(options?.timeout ?? Duration.hours(24)))
                  : yield* timeoutAt(state.timeoutAt, options?.timeout ?? Duration.hours(24)),
              param: yield* encodeTargetPayload(targetFunction, args, options),
              tags: Protocol.Tags.make({
                reserved: {
                  ...(options?.tags ?? Protocol.emptyTags).reserved,
                  "resonate:origin": mode === "attached" && Predicate.isUndefined(options?.id) ? state.originId : id,
                  "resonate:prefix": state.prefixId,
                  "resonate:branch": id,
                  "resonate:parent": state.root,
                  "resonate:scope": globalScope,
                  "resonate:target": target,
                },
                unrecognized: options?.tags?.unrecognized ?? {},
                user: options?.tags?.user ?? {},
              }),
            },
          }),
        });
        addPreload(state, result.preload);
        const promise = yield* actionPromise(result.action);
        state.cache.set(promise.id, promise);
        if (mode === "attached" && promise.state === "pending") {
          state.attachedRemote.set(promise.id, new SuspendedExecution({ awaited: [promise.id] }));
        }
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

      const makePromiseHandle = <P extends PromiseDeclaration>(
        state: RuntimeState,
        promise: Protocol.PromiseRecord,
        declaration: P,
      ): LocalDurableHandle<PromiseSuccess<P>, unknown> => {
        const awaitExternal: Effect.Effect<PromiseSuccess<P>, unknown> = Effect.suspend(() => {
          if (promise.state === "pending") {
            const parked = state.awaiting.get(promise.id);
            if (Predicate.isNotUndefined(parked)) {
              return Effect.fail(parked);
            }
            const suspended = new SuspendedExecution({ awaited: [promise.id] });
            state.awaiting.set(promise.id, suspended);
            return Effect.fail(suspended);
          }
          if (promise.state === "rejected_canceled") {
            return new DurablePromiseCanceled({ id: promise.id });
          }
          if (promise.state === "rejected_timedout") {
            return new DurablePromiseTimedOut({ id: promise.id });
          }
          return codec.decode(promise.value).pipe(
            Effect.catch((cause) =>
              Effect.die(new EncodingError({ direction: "decode", id: Option.some(promise.id), cause })),
            ),
            Effect.flatMap((value) => {
              if (promise.state === "resolved") {
                return Schema.decodeUnknownEffect(declaration.success)(value).pipe(
                  Effect.catch((cause) =>
                    Effect.die(new EncodingError({ direction: "decode", id: Option.some(promise.id), cause })),
                  ),
                );
              }
              if (Predicate.isUndefined(declaration.error)) {
                return Effect.die(
                  new EncodingError({ direction: "decode", id: Option.some(promise.id), cause: value }),
                );
              }
              return Schema.decodeUnknownEffect(declaration.error)(value).pipe(
                Effect.catch((cause) =>
                  Effect.die(new EncodingError({ direction: "decode", id: Option.some(promise.id), cause })),
                ),
                Effect.flatMap(Effect.fail),
              );
            }),
          );
        });
        return {
          id: promise.id,
          await: awaitExternal,
          poll: awaitExternal.pipe(Effect.exit, Effect.map(Option.some)),
          cancel: settle(state, promise.id, Exit.fail(new DurablePromiseCanceled({ id: promise.id }))).pipe(
            Effect.asVoid,
          ),
        };
      };

      const runWithRetry = Effect.fn("ExecutionEngine.runWithRetry")(function* (
        state: RuntimeState,
        effect: Effect.Effect<unknown, unknown>,
        policy: RetryPolicy.RetryPolicy,
        nonRetryableErrors: ReadonlyArray<Schema.Codec<unknown, unknown, never, never>>,
      ): Effect.fn.Return<Exit.Exit<unknown, unknown>> {
        const runAttempt = Effect.fn("ExecutionEngine.runWithRetry.runAttempt")(function* (
          attempt: number,
        ): Effect.fn.Return<Exit.Exit<unknown, unknown>> {
          state.attempt = attempt;
          const result = yield* effect.pipe(Effect.result);
          if (Result.isSuccess(result)) {
            return Exit.succeed(result.success);
          }
          if (nonRetryableErrors.some((schema) => SchemaParser.is(schema)(result.failure))) {
            return Exit.fail(result.failure);
          }
          const retryIn = RetryPolicy.next(policy, attempt);
          const now = yield* Clock.currentTimeMillis;
          if (Predicate.isNull(retryIn) || now + retryIn >= DateTime.toEpochMillis(state.timeoutAt)) {
            return Exit.fail(result.failure);
          }
          if (retryIn > 0) {
            yield* Effect.sleep(Duration.millis(retryIn));
          }
          return yield* runAttempt(attempt + 1);
        });
        return yield* runAttempt(0);
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
          const fiber = yield* runWithRetry(
            state,
            effect,
            options?.retryPolicy ?? RetryPolicy.exponential(),
            options?.nonRetryableErrors ?? [],
          ).pipe(
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

        const sleepUntil: ResonateContextService["sleepUntil"] = Effect.fn("ResonateContext.sleepUntil")(
          function* (instant) {
            const promise = yield* createSleep(state, instant);
            if (promise.state !== "pending") {
              yield* decodeSettled(promise);
              return;
            }
            const parked = state.awaiting.get(promise.id);
            if (Predicate.isNotUndefined(parked)) {
              return yield* Effect.fail(parked);
            }
            const suspended = new SuspendedExecution({ awaited: [promise.id] });
            state.awaiting.set(promise.id, suspended);
            return yield* suspended;
          },
        );

        const sleep: ResonateContextService["sleep"] = Effect.fn("ResonateContext.sleep")(function* (duration) {
          return yield* sleepUntil(timestamp((yield* Clock.currentTimeMillis) + Duration.toMillis(duration)));
        });

        const beginRpcImpl = Effect.fn("ResonateContext.beginRpc")(function* (
          targetFunction: AnyFunction | string,
          args: ReadonlyArray<unknown>,
          options?: ContextOptions,
        ): Effect.fn.Return<LocalDurableHandle, unknown> {
          const promise = yield* createRemote(state, targetFunction, args, options, "attached");
          const deferred = yield* Deferred.make<unknown, unknown>();
          if (promise.state !== "pending") {
            const settled = yield* decodeSettled(promise).pipe(Effect.exit);
            yield* Deferred.done(deferred, settled);
          }
          return makeHandle(state, promise, deferred);
        });
        const beginRpc: ResonateContextService["beginRpc"] = beginRpcImpl;

        const rpc: ResonateContextService["rpc"] = Effect.fn("ResonateContext.rpc")(function* (
          targetFunction: AnyFunction | string,
          args: ReadonlyArray<unknown>,
          options?: ContextOptions,
        ): Effect.fn.Return<unknown, unknown> {
          const handle = yield* beginRpcImpl(targetFunction, args, options);
          return yield* handle.await;
        });

        const detached: ResonateContextService["detached"] = Effect.fn("ResonateContext.detached")(function* (
          targetFunction: AnyFunction | string,
          args: ReadonlyArray<unknown>,
          options?: ContextOptions,
        ): Effect.fn.Return<LocalDurableHandle, unknown> {
          const promise = yield* createRemote(state, targetFunction, args, options, "detached");
          const deferred = yield* Deferred.make<unknown, unknown>();
          if (promise.state !== "pending") {
            const settled = yield* decodeSettled(promise).pipe(Effect.exit);
            yield* Deferred.done(deferred, settled);
          }
          return makeHandle(state, promise, deferred);
        });

        const promise: ResonateContextService["promise"] = Effect.fn("ResonateContext.promise")(
          function* (declaration, options) {
            const promise = yield* createExternalPromise(state, declaration, options);
            return makePromiseHandle(state, promise, declaration);
          },
        );

        return Layer.succeed(
          ResonateContext,
          ResonateContext.of({
            info: {
              get attempt() {
                return state.attempt;
              },
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
            sleep,
            sleepUntil,
            beginRpc,
            rpc,
            detached,
            promise,
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
        const rootTarget = options.promise.tags.reserved["resonate:target"];
        const state: RuntimeState = {
          root: options.promise.id,
          version: options.task.version,
          timeoutAt: options.promise.timeoutAt,
          targetTransport: rootTarget?.transport ?? "poll",
          targetGroup: rootTarget?.group ?? Protocol.WorkerGroup.make("default"),
          originId: options.promise.tags.reserved["resonate:origin"] ?? options.promise.id,
          prefixId: options.promise.tags.reserved["resonate:prefix"] ?? options.promise.id,
          parentId: options.promise.tags.reserved["resonate:parent"] ?? options.promise.id,
          branchId: options.promise.tags.reserved["resonate:branch"] ?? options.promise.id,
          cache: new Map(),
          children: [],
          attachedRemote: new Map(),
          awaiting: new Map(),
          externalPromises: new Set(),
          attempt: 0,
          seq: 0,
        };
        addPreload(state, options.preload ?? []);

        if (options.promise.state !== "pending") {
          return new EngineDone({ promise: options.promise });
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
        const attachedAwaited = [...state.attachedRemote.keys()];
        if (Exit.isSuccess(exit)) {
          if (Predicate.isTagged(exit.value, "SuspendedExecution")) {
            return new EngineSuspended({ awaited: [...new Set([...attachedAwaited, ...exit.value.awaited])] });
          }
          if (attachedAwaited.length > 0) {
            return new EngineSuspended({ awaited: [...new Set(attachedAwaited)] });
          }
          const promise = yield* fulfillRoot(state, Exit.succeed(exit.value.value));
          return new EngineDone({ promise });
        }
        const promise = yield* fulfillRoot(state, exit);
        return new EngineDone({ promise });
      });

      return ExecutionEngine.of({ execute });
    }),
  );
}
