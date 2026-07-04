/**
 * Durable execution context used inside Resonate function handlers.
 *
 * The context records local steps, durable sleeps, child invocations, detached
 * work, and external promises as protocol operations so execution can suspend
 * and replay deterministically.
 *
 * @since 0.0.0
 */
import {
  Array as Arr,
  Clock,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  HashMap,
  HashSet,
  Layer,
  Match,
  Number as Num,
  Option,
  Predicate,
  Random,
  Result,
  Schema,
  SchemaParser,
} from "effect";
import { currentCodec, withSchemaHeader } from "./Codec.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, EncodingError } from "./Errors.ts";
import { InvocationParam, type AnyFunction, type PayloadArgs } from "./FunctionDefinition.ts";
import * as Protocol from "./Protocol.ts";
import type { PromiseDeclaration, PromiseSuccess } from "./PromiseDefinition.ts";
import type { Registry } from "./Registry.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import { Tasks } from "./Task.ts";

export class DurablePanic extends Schema.TaggedErrorClass<DurablePanic>()("DurablePanic", {
  message: Schema.String,
}) {}

class SuspendedExecution extends Schema.TaggedErrorClass<SuspendedExecution>()("SuspendedExecution", {
  awaited: Schema.Array(Protocol.PromiseId),
}) {}

const isSuspendedExecution = SchemaParser.is(SuspendedExecution);

/**
 * Options shared by durable context operations.
 *
 * @category models
 * @since 0.0.0
 */
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

/**
 * Handle returned for durable work created from inside a handler.
 *
 * @category models
 * @since 0.0.0
 */
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
  cache: HashMap.HashMap<Protocol.PromiseId, Protocol.PromiseRecord>;
  readonly children: Array<RunningChild>;
  attachedRemote: HashMap.HashMap<Protocol.PromiseId, SuspendedExecution>;
  awaiting: HashMap.HashMap<Protocol.PromiseId, SuspendedExecution>;
  externalPromises: HashSet.HashSet<string>;
  attempt: number;
  seq: number;
}

namespace RuntimeState {
  export interface MakeOptions {
    readonly task: Protocol.TaskAcquired;
    readonly promise: Protocol.PromiseRecord;
    readonly preload?: ReadonlyArray<Protocol.PromiseRecord>;
  }

  export const make = (options: MakeOptions): RuntimeState => {
    const rootTarget = options.promise.tags.reserved["resonate:target"];
    return {
      root: options.promise.id,
      version: options.task.version,
      timeoutAt: options.promise.timeoutAt,
      targetTransport: rootTarget?.transport ?? "poll",
      targetGroup: rootTarget?.group ?? Protocol.WorkerGroup.make("default"),
      originId: Protocol.promiseOrigin(options.promise),
      prefixId: options.promise.tags.reserved["resonate:prefix"] ?? options.promise.id,
      parentId: options.promise.tags.reserved["resonate:parent"] ?? options.promise.id,
      branchId: options.promise.tags.reserved["resonate:branch"] ?? options.promise.id,
      cache: HashMap.fromIterable(Arr.map(options.preload ?? [], (promise) => [promise.id, promise] as const)),
      children: [],
      attachedRemote: HashMap.empty(),
      awaiting: HashMap.empty(),
      externalPromises: HashSet.empty(),
      attempt: 0,
      seq: 0,
    };
  };
}

interface SettleOptions {
  readonly id: Protocol.PromiseId;
  readonly exit: Exit.Exit<unknown, unknown>;
}

interface FulfillRootOptions {
  readonly exit: Exit.Exit<unknown, unknown>;
}

interface FenceOptions {
  readonly action: Protocol.TaskFenceRequest["data"]["action"];
}

interface TimeoutAtOptions {
  readonly parent: Protocol.Timestamp;
  readonly duration: Duration.Duration;
}

interface EncodeInvocationOptions {
  readonly name: string;
  readonly args: ReadonlyArray<unknown>;
  readonly version: Protocol.FunctionVersionOrLatest;
  readonly retry?: RetryPolicy.RetryPolicy;
}

interface EncodeTargetPayloadOptions {
  readonly target: AnyFunction | string;
  readonly args: ReadonlyArray<unknown>;
  readonly options?: ContextOptions;
}

interface LocalTagsOptions {
  readonly id: Protocol.PromiseId;
  readonly extra: Protocol.Tags;
  readonly breaksLineage: boolean;
}

interface CreateLocalOptions {
  readonly options: ContextOptions | undefined;
}

interface CreateSleepOptions {
  readonly instant: DateTime.Utc;
}

interface CreateExternalPromiseOptions {
  readonly declaration: PromiseDeclaration;
  readonly options: Pick<ContextOptions, "id" | "timeout" | "tags"> | undefined;
}

interface CreateRemoteOptions {
  readonly targetFunction: AnyFunction | string;
  readonly args: ReadonlyArray<unknown>;
  readonly options: ContextOptions | undefined;
  readonly mode: "attached" | "detached";
}

interface MakeHandleOptions {
  readonly promise: Protocol.PromiseRecord;
  readonly deferred: Deferred.Deferred<unknown, unknown>;
}

interface MakePromiseHandleOptions<P extends PromiseDeclaration> {
  readonly promise: Protocol.PromiseRecord;
  readonly declaration: P;
}

interface RunWithRetryOptions {
  readonly effect: Effect.Effect<unknown, unknown>;
  readonly policy: RetryPolicy.RetryPolicy;
  readonly nonRetryableErrors: ReadonlyArray<Schema.Codec<unknown, unknown, never, never>>;
}

interface BeginRpcOptions {
  readonly target: AnyFunction | string;
  readonly args: ReadonlyArray<unknown>;
  readonly options?: ContextOptions;
}

interface BeginRunOptions {
  readonly effect: Effect.Effect<unknown, unknown>;
  readonly options?: ContextOptions;
}

interface PromiseOptions<P extends PromiseDeclaration> {
  readonly declaration: P;
  readonly options?: Pick<ContextOptions, "id" | "timeout" | "tags">;
}

interface ChildIdOptions {
  readonly parent: Protocol.PromiseId;
  readonly seq: number;
}

interface RequestHeadOptions {
  readonly corrId: string;
  readonly origin?: Protocol.PromiseId;
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

const childId = (options: ChildIdOptions): Protocol.PromiseId => {
  const { parent, seq } = options;
  return Protocol.PromiseId.make(`${parent}.${seq}`);
};

const requestHead = (options: RequestHeadOptions): Protocol.RequestHead => {
  const { corrId, origin } = options;
  return Protocol.RequestHead.make({
    corrId: Protocol.CorrelationId.make(corrId),
    version: Protocol.protocolVersion,
    ...(Predicate.isNotUndefined(origin) ? { "resonate:origin": origin } : {}),
  });
};

const isExternalPromise = (promise: Protocol.PromiseRecord): boolean =>
  Predicate.isNotUndefined(promise.tags.reserved["resonate:target"]) ||
  Predicate.isNotUndefined(promise.tags.reserved["resonate:timer"]);

const isPromiseCreateSuccess = SchemaParser.is(Protocol.PromiseCreateSuccessResponse);
const isPromiseSettleSuccess = SchemaParser.is(Protocol.PromiseSettleSuccessResponse);

export interface ResonateContextService {
  readonly info: ContextInfo;
  readonly run: (options: {
    readonly effect: Effect.Effect<unknown, unknown>;
    readonly options?: ContextOptions;
  }) => Effect.Effect<unknown, unknown>;
  readonly beginRun: (options: {
    readonly effect: Effect.Effect<unknown, unknown>;
    readonly options?: ContextOptions;
  }) => Effect.Effect<LocalDurableHandle, unknown>;
  readonly all: <const Effects extends ReadonlyArray<Effect.Effect<unknown, unknown>>>(
    effects: Effects,
  ) => Effect.Effect<ReadonlyArray<unknown>, unknown>;
  readonly now: Effect.Effect<DateTime.Utc, unknown>;
  readonly random: Effect.Effect<number, unknown>;
  readonly sleep: (duration: Duration.Input) => Effect.Effect<void, unknown>;
  readonly sleepUntil: (instant: DateTime.Utc) => Effect.Effect<void, unknown>;
  readonly beginRpc: {
    <F extends AnyFunction>(options: {
      readonly target: F;
      readonly args: PayloadArgs<F>;
      readonly options?: ContextOptions;
    }): Effect.Effect<LocalDurableHandle, unknown>;
    (options: {
      readonly target: string;
      readonly args: ReadonlyArray<unknown>;
      readonly options?: ContextOptions;
    }): Effect.Effect<LocalDurableHandle, unknown>;
  };
  readonly rpc: {
    <F extends AnyFunction>(options: {
      readonly target: F;
      readonly args: PayloadArgs<F>;
      readonly options?: ContextOptions;
    }): Effect.Effect<unknown, unknown>;
    (options: {
      readonly target: string;
      readonly args: ReadonlyArray<unknown>;
      readonly options?: ContextOptions;
    }): Effect.Effect<unknown, unknown>;
  };
  readonly detached: {
    <F extends AnyFunction>(options: {
      readonly target: F;
      readonly args: PayloadArgs<F>;
      readonly options?: ContextOptions;
    }): Effect.Effect<LocalDurableHandle, unknown>;
    (options: {
      readonly target: string;
      readonly args: ReadonlyArray<unknown>;
      readonly options?: ContextOptions;
    }): Effect.Effect<LocalDurableHandle, unknown>;
  };
  readonly promise: <P extends PromiseDeclaration>(options: {
    readonly declaration: P;
    readonly options?: Pick<ContextOptions, "id" | "timeout" | "tags">;
  }) => Effect.Effect<LocalDurableHandle<PromiseSuccess<P>, unknown>, unknown>;
  readonly panic: (message: string) => Effect.Effect<never, DurablePanic>;
}

/**
 * Service available inside a registered handler for durable operations.
 *
 * @category services
 * @since 0.0.0
 */
export class ResonateContext extends Context.Service<ResonateContext, ResonateContextService>()(
  "effect-resonate/Context",
) {}

export interface ExecutionEngineService {
  readonly execute: (options: ExecuteOptions) => Effect.Effect<EngineOutcome, unknown>;
}

/**
 * Internal execution engine that interprets handler effects into task outcomes.
 *
 * @category services
 * @since 0.0.0
 */
export class ExecutionEngine extends Context.Service<ExecutionEngine, ExecutionEngineService>()(
  "effect-resonate/ExecutionEngine",
) {
  static readonly layer: Layer.Layer<ExecutionEngine, never, Tasks> = Layer.effect(
    ExecutionEngine,
    Effect.gen(function* () {
      const tasks = yield* Tasks;
      const codec = yield* currentCodec;

      const decodeSettled = Effect.fn("ExecutionEngine.decodeSettled")(function* (promise: Protocol.PromiseRecord) {
        return yield* Match.value(promise).pipe(
          Match.when({ state: "pending" }, (promise) => Effect.die(`Promise '${promise.id}' is still pending`)),
          Match.when({ state: "rejected_canceled" }, (promise) => new DurablePromiseCanceled({ id: promise.id })),
          Match.when({ state: "rejected_timedout" }, (promise) => new DurablePromiseTimedOut({ id: promise.id })),
          Match.when({ state: "resolved" }, (promise) => codec.decode(promise.value)),
          Match.when({ state: "rejected" }, (promise) => codec.decode(promise.value).pipe(Effect.flatMap(Effect.fail))),
          Match.exhaustive,
        );
      });

      const makeSession = (state: RuntimeState) => {
        const fence = Effect.fn("ExecutionEngine.fence")(function* (options: FenceOptions) {
          const { action } = options;
          const result = yield* tasks.fence({
            data: { id: state.root, version: state.version, action },
            options: { origin: state.originId },
          });
          state.cache = HashMap.setMany(
            state.cache,
            Arr.map(result.preload, (promise) => [promise.id, promise] as const),
          );
          if (!isPromiseCreateSuccess(result.action) && !isPromiseSettleSuccess(result.action)) {
            return yield* Effect.die(result.action);
          }
          const promise = result.action.data.promise;
          state.cache = HashMap.set(state.cache, promise.id, promise);
          return promise;
        });

        const settle = Effect.fn("ExecutionEngine.settle")(function* (options: SettleOptions) {
          const { id, exit } = options;
          const settled = Exit.isSuccess(exit) ? resolvedState : rejectedState;
          const value = yield* codec.encode(Exit.isSuccess(exit) ? exit.value : exit.cause);
          return yield* fence({
            action: Protocol.PromiseSettleRequest.make({
              head: requestHead({ corrId: `${state.root}:${id}:settle`, origin: state.originId }),
              data: { id, state: settled, value },
            }),
          });
        });

        const fulfillRoot = Effect.fn("ExecutionEngine.fulfillRoot")(function* (options: FulfillRootOptions) {
          const { exit } = options;
          const settled = Exit.isSuccess(exit) ? resolvedState : rejectedState;
          const value = yield* codec.encode(Exit.isSuccess(exit) ? exit.value : exit.cause);
          const promise = yield* tasks.fulfill({
            data: {
              id: state.root,
              version: state.version,
              action: Protocol.PromiseSettleRequest.make({
                head: requestHead({ corrId: `${state.root}:fulfill`, origin: state.originId }),
                data: { id: state.root, state: settled, value },
              }),
            },
            options: { origin: state.originId },
          });
          state.cache = HashMap.set(state.cache, promise.id, promise);
          return promise;
        });

        const timeoutAt = Effect.fn("ExecutionEngine.timeoutAt")(function* (options: TimeoutAtOptions) {
          const { parent, duration } = options;
          const now = yield* Clock.currentTimeMillis;
          return timestamp(Num.min(Num.sum(now, Duration.toMillis(duration)), DateTime.toEpochMillis(parent)));
        });

        const encodeInvocation = Effect.fn("ExecutionEngine.encodeInvocation")(function* (
          options: EncodeInvocationOptions,
        ): Effect.fn.Return<Protocol.Value, unknown> {
          const { name, args, version, retry } = options;
          const encoded = yield* codec.encode(
            InvocationParam.make({
              func: name,
              args,
              version,
              ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
            }),
          );
          return withSchemaHeader({ value: encoded, schemaName: name });
        });

        const encodeTargetPayload = Effect.fn("ExecutionEngine.encodeTargetPayload")(function* (
          input: EncodeTargetPayloadOptions,
        ): Effect.fn.Return<Protocol.Value, unknown> {
          const { target, args, options } = input;
          if (Predicate.isString(target)) {
            return yield* encodeInvocation({
              name: target,
              args,
              version: Predicate.isUndefined(options?.version) ? Protocol.FunctionVersion.make(1) : options.version,
              retry: options?.retryPolicy,
            });
          }
          const encodedArgs = yield* Schema.encodeUnknownEffect(target.payload)(args).pipe(
            Effect.catchCause(() =>
              args.length === 1
                ? Schema.encodeUnknownEffect(target.payload)(args[0])
                : Effect.die("Invalid function payload"),
            ),
          );
          return yield* encodeInvocation({
            name: target.name,
            args: Arr.ensure(encodedArgs),
            version: options?.version ?? target.version,
            retry: options?.retryPolicy,
          });
        });

        const localTags = (options: LocalTagsOptions): Protocol.Tags => {
          const { id, extra, breaksLineage } = options;
          return Protocol.Tags.make({
            reserved: {
              ...extra.reserved,
              "resonate:origin": breaksLineage ? id : state.originId,
              "resonate:prefix": state.prefixId,
              "resonate:branch": state.branchId,
              "resonate:parent": state.root,
              "resonate:scope": localScope,
            },
            unrecognized: extra.unrecognized,
            user: extra.user,
          });
        };

        const createLocal = Effect.fn("ExecutionEngine.createLocal")(function* (input: CreateLocalOptions) {
          const { options } = input;
          const id = options?.id ?? childId({ parent: state.root, seq: state.seq });
          state.seq = Num.increment(state.seq);
          const cached = HashMap.get(state.cache, id);
          if (Option.isSome(cached)) {
            return cached.value;
          }
          return yield* fence({
            action: Protocol.PromiseCreateRequest.make({
              head: requestHead({ corrId: `${state.root}:${id}:create`, origin: state.originId }),
              data: {
                id,
                timeoutAt: yield* timeoutAt({
                  parent: state.timeoutAt,
                  duration: options?.timeout ?? Duration.hours(24),
                }),
                param: Protocol.emptyValue,
                tags: localTags({
                  id,
                  extra: options?.tags ?? Protocol.emptyTags,
                  breaksLineage: Predicate.isNotUndefined(options?.id),
                }),
              },
            }),
          });
        });

        const createSleep = Effect.fn("ExecutionEngine.createSleep")(function* (options: CreateSleepOptions) {
          const { instant } = options;
          const id = childId({ parent: state.root, seq: state.seq });
          state.seq = Num.increment(state.seq);
          const cached = HashMap.get(state.cache, id);
          if (Option.isSome(cached)) {
            return cached.value;
          }
          return yield* fence({
            action: Protocol.PromiseCreateRequest.make({
              head: requestHead({ corrId: `${state.root}:${id}:sleep`, origin: state.originId }),
              data: {
                id,
                timeoutAt: timestamp(Num.min(DateTime.toEpochMillis(instant), DateTime.toEpochMillis(state.timeoutAt))),
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
        });

        const createExternalPromise = Effect.fn("ExecutionEngine.createExternalPromise")(function* (
          input: CreateExternalPromiseOptions,
        ) {
          const { declaration, options } = input;
          if (Predicate.isUndefined(options?.id)) {
            if (HashSet.has(state.externalPromises, declaration.name)) {
              return yield* Effect.die(`Promise declaration '${declaration.name}' was created more than once`);
            }
            state.externalPromises = HashSet.add(state.externalPromises, declaration.name);
          }
          const id = options?.id ?? declaration.id(state.root);
          const cached = HashMap.get(state.cache, id);
          if (Option.isSome(cached)) {
            if (cached.value.state === "pending") {
              state.attachedRemote = HashMap.set(
                state.attachedRemote,
                cached.value.id,
                new SuspendedExecution({ awaited: [cached.value.id] }),
              );
            }
            return cached.value;
          }
          const promise = yield* fence({
            action: Protocol.PromiseCreateRequest.make({
              head: requestHead({ corrId: `${state.root}:${id}:promise`, origin: state.originId }),
              data: {
                id,
                timeoutAt: yield* timeoutAt({
                  parent: state.timeoutAt,
                  duration: options?.timeout ?? Duration.hours(24),
                }),
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
          if (promise.state === "pending") {
            state.attachedRemote = HashMap.set(
              state.attachedRemote,
              promise.id,
              new SuspendedExecution({ awaited: [promise.id] }),
            );
          }
          return promise;
        });

        const createRemote = Effect.fn("ExecutionEngine.createRemote")(function* (input: CreateRemoteOptions) {
          const { targetFunction, args, options, mode } = input;
          const seqid = childId({ parent: state.root, seq: state.seq });
          const id =
            options?.id ??
            (mode === "detached" ? Protocol.detachedPromiseId({ prefix: state.prefixId, seqid }) : seqid);
          state.seq = Num.increment(state.seq);
          const cached = HashMap.get(state.cache, id);
          if (Option.isSome(cached)) {
            if (mode === "attached" && cached.value.state === "pending") {
              state.attachedRemote = HashMap.set(
                state.attachedRemote,
                cached.value.id,
                new SuspendedExecution({ awaited: [cached.value.id] }),
              );
            }
            return cached.value;
          }
          const targetGroup = options?.target ?? state.targetGroup;
          const target =
            state.targetTransport === "local"
              ? Protocol.TargetAddress.localAny({ group: targetGroup })
              : Protocol.TargetAddress.pollAny({ group: targetGroup });
          const now = yield* Clock.currentTimeMillis;
          const promise = yield* fence({
            action: Protocol.PromiseCreateRequest.make({
              head: requestHead({ corrId: `${state.root}:${id}:rpc`, origin: state.originId }),
              data: {
                id,
                timeoutAt:
                  mode === "detached"
                    ? timestamp(now + Duration.toMillis(options?.timeout ?? Duration.hours(24)))
                    : yield* timeoutAt({ parent: state.timeoutAt, duration: options?.timeout ?? Duration.hours(24) }),
                param: yield* encodeTargetPayload({ target: targetFunction, args, options }),
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
          if (mode === "attached" && promise.state === "pending") {
            state.attachedRemote = HashMap.set(
              state.attachedRemote,
              promise.id,
              new SuspendedExecution({ awaited: [promise.id] }),
            );
          }
          return promise;
        });

        const makeHandle = (options: MakeHandleOptions): LocalDurableHandle => {
          const { promise, deferred } = options;
          return {
            id: promise.id,
            await:
              isExternalPromise(promise) && promise.state === "pending"
                ? Effect.suspend(() => {
                    const parked = HashMap.get(state.awaiting, promise.id);
                    if (Option.isSome(parked)) {
                      return Effect.fail(parked.value);
                    }
                    const suspended = new SuspendedExecution({ awaited: [promise.id] });
                    state.awaiting = HashMap.set(state.awaiting, promise.id, suspended);
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
            cancel: settle({ id: promise.id, exit: Exit.fail(new DurablePromiseCanceled({ id: promise.id })) }).pipe(
              Effect.asVoid,
            ),
          };
        };

        const makePromiseHandle = <P extends PromiseDeclaration>(
          options: MakePromiseHandleOptions<P>,
        ): LocalDurableHandle<PromiseSuccess<P>, unknown> => {
          const { promise, declaration } = options;
          const awaitExternal: Effect.Effect<PromiseSuccess<P>, unknown> = Effect.suspend(() => {
            if (promise.state === "pending") {
              const parked = HashMap.get(state.awaiting, promise.id);
              if (Option.isSome(parked)) {
                return Effect.fail(parked.value);
              }
              const suspended = new SuspendedExecution({ awaited: [promise.id] });
              state.awaiting = HashMap.set(state.awaiting, promise.id, suspended);
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
            cancel: settle({
              id: promise.id,
              exit: Exit.fail(new DurablePromiseCanceled({ id: promise.id })),
            }).pipe(Effect.asVoid),
          };
        };

        const runWithRetry = Effect.fn("ExecutionEngine.runWithRetry")(function* (
          options: RunWithRetryOptions,
        ): Effect.fn.Return<Exit.Exit<unknown, unknown>> {
          const { effect, policy, nonRetryableErrors } = options;
          const runAttempt = Effect.fn("ExecutionEngine.runWithRetry.runAttempt")(function* (
            attempt: number,
          ): Effect.fn.Return<Exit.Exit<unknown, unknown>> {
            state.attempt = attempt;
            const result = yield* effect.pipe(Effect.result);
            if (Result.isSuccess(result)) {
              return Exit.succeed(result.success);
            }
            if (Arr.some(nonRetryableErrors, (schema) => SchemaParser.is(schema)(result.failure))) {
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
            return yield* runAttempt(Num.increment(attempt));
          });
          return yield* runAttempt(0);
        });

        const drainChildren = Effect.fn("ExecutionEngine.drainChildren")(function* () {
          for (const child of state.children) {
            yield* Fiber.join(child.fiber).pipe(Effect.exit);
          }
        });

        const layer = Layer.suspend((): Layer.Layer<ResonateContext, never, never> => {
          const beginRpcImpl = Effect.fn("ResonateContext.beginRpc")(function* (
            input: BeginRpcOptions,
          ): Effect.fn.Return<LocalDurableHandle, unknown> {
            const { target, args, options } = input;
            const promise = yield* createRemote({ targetFunction: target, args, options, mode: "attached" });
            const deferred = yield* Deferred.make<unknown, unknown>();
            if (promise.state !== "pending") {
              const settled = yield* decodeSettled(promise).pipe(Effect.exit);
              yield* Deferred.done(deferred, settled);
            }
            return makeHandle({ promise, deferred });
          });

          const service = ResonateContext.of({
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
            beginRun: Effect.fn("ResonateContext.beginRun")(function* (
              input: BeginRunOptions,
            ): Effect.fn.Return<LocalDurableHandle, unknown> {
              const { effect, options } = input;
              const promise = yield* createLocal({ options });
              const deferred = yield* Deferred.make<unknown, unknown>();
              if (promise.state !== "pending") {
                const settled = yield* decodeSettled(promise).pipe(Effect.exit);
                yield* Deferred.done(deferred, settled);
                return makeHandle({ promise, deferred });
              }
              if (isExternalPromise(promise)) {
                return makeHandle({ promise, deferred });
              }
              const fiber = yield* runWithRetry({
                effect,
                policy: options?.retryPolicy ?? RetryPolicy.exponential(),
                nonRetryableErrors: options?.nonRetryableErrors ?? [],
              }).pipe(
                Effect.flatMap((exit) => settle({ id: promise.id, exit })),
                Effect.flatMap((settled) =>
                  decodeSettled(settled).pipe(
                    Effect.exit,
                    Effect.flatMap((exit) => Deferred.done(deferred, exit)),
                  ),
                ),
                Effect.forkDetach,
              );
              state.children.push({ id: promise.id, fiber });
              return makeHandle({ promise, deferred });
            }),
            run: Effect.fn("ResonateContext.run")(function* (input: BeginRunOptions) {
              const { effect, options } = input;
              const handle = yield* service.beginRun({ effect, options });
              return yield* handle.await;
            }),
            get now() {
              return service
                .run({ effect: Clock.currentTimeMillis })
                .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)), Effect.map(timestamp));
            },
            get random() {
              return service
                .run({ effect: Random.next })
                .pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Finite)));
            },
            sleepUntil: Effect.fn("ResonateContext.sleepUntil")(function* (instant) {
              const promise = yield* createSleep({ instant });
              if (promise.state !== "pending") {
                yield* decodeSettled(promise);
                return;
              }
              const parked = HashMap.get(state.awaiting, promise.id);
              if (Option.isSome(parked)) {
                return yield* parked.value;
              }
              const suspended = new SuspendedExecution({ awaited: [promise.id] });
              state.awaiting = HashMap.set(state.awaiting, promise.id, suspended);
              return yield* suspended;
            }),
            sleep: Effect.fn("ResonateContext.sleep")(function* (duration) {
              return yield* service.sleepUntil(
                timestamp((yield* Clock.currentTimeMillis) + Duration.toMillis(duration)),
              );
            }),
            beginRpc: beginRpcImpl,
            rpc: Effect.fn("ResonateContext.rpc")(function* (
              input: BeginRpcOptions,
            ): Effect.fn.Return<unknown, unknown> {
              const { target, args, options } = input;
              const handle = yield* beginRpcImpl({ target, args, options });
              return yield* handle.await;
            }),
            detached: Effect.fn("ResonateContext.detached")(function* (
              input: BeginRpcOptions,
            ): Effect.fn.Return<LocalDurableHandle, unknown> {
              const { target, args, options } = input;
              const targetFunction = target;
              const promise = yield* createRemote({ targetFunction, args, options, mode: "detached" });
              const deferred = yield* Deferred.make<unknown, unknown>();
              if (promise.state !== "pending") {
                const settled = yield* decodeSettled(promise).pipe(Effect.exit);
                yield* Deferred.done(deferred, settled);
              }
              return makeHandle({ promise, deferred });
            }),
            promise: Effect.fn("ResonateContext.promise")(function* <P extends PromiseDeclaration>(
              input: PromiseOptions<P>,
            ) {
              const { declaration, options } = input;
              const promise = yield* createExternalPromise({ declaration, options });
              return makePromiseHandle({ promise, declaration });
            }),
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
          });

          return Layer.succeed(ResonateContext, service);
        });

        return {
          layer,
          drainChildren,
          fulfillRoot,
          attachedAwaited: () => Array.from(HashMap.keys(state.attachedRemote)),
        };
      };

      return ExecutionEngine.of({
        execute: Effect.fn("ExecutionEngine.execute")(function* (options) {
          const state = RuntimeState.make({ task: options.task, promise: options.promise, preload: options.preload });
          const session = makeSession(state);

          if (options.promise.state !== "pending") {
            return new EngineDone({ promise: options.promise });
          }

          const invocation = yield* codec
            .decode(options.promise.param)
            .pipe(Effect.flatMap(Schema.decodeUnknownEffect(InvocationParam)));
          const item = yield* Option.match(
            options.registry.get({ name: invocation.func, version: invocation.version }),
            {
              onNone: () => Effect.die(`Function '${invocation.func}' is not registered`),
              onSome: Effect.succeed,
            },
          );
          const decoded = yield* Schema.decodeUnknownEffect(item.definition.payload)(invocation.args).pipe(
            Effect.catchCause(() => Schema.decodeUnknownEffect(item.definition.payload)(invocation.args[0])),
          );
          const args = Arr.ensure(decoded);
          const result: Effect.Effect<unknown, unknown> = Reflect.apply(item.handler, undefined, args);
          if (!Effect.isEffect(result)) {
            return yield* Effect.die(`Function '${invocation.func}' did not return an Effect`);
          }
          const exit = yield* result.pipe(
            Effect.provide(session.layer),
            Effect.map((value) => new CompletedExecution({ value })),
            Effect.catch((error) => (isSuspendedExecution(error) ? Effect.succeed(error) : Effect.fail(error))),
            Effect.exit,
          );
          yield* session.drainChildren();
          const attachedAwaited = session.attachedAwaited();
          if (Exit.isSuccess(exit)) {
            if (Predicate.isTagged(exit.value, "SuspendedExecution")) {
              return new EngineSuspended({ awaited: Arr.dedupe([...attachedAwaited, ...exit.value.awaited]) });
            }
            if (attachedAwaited.length > 0) {
              return new EngineSuspended({ awaited: Arr.dedupe(attachedAwaited) });
            }
            const promise = yield* session.fulfillRoot({ exit: Exit.succeed(exit.value.value) });
            return new EngineDone({ promise });
          }
          const promise = yield* session.fulfillRoot({ exit });
          return new EngineDone({ promise });
        }),
      });
    }),
  );
}
