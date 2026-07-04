/**
 * Resonate client service for starting and controlling durable work.
 *
 * @since 0.0.0
 */
import {
  Array as Arr,
  Clock,
  Context,
  Crypto,
  Duration,
  Effect,
  Exit,
  Layer,
  Match,
  Option,
  Predicate,
  Schema,
} from "effect";
import { currentCodec, withSchemaHeader } from "./Codec.ts";
import { DurablePromises } from "./DurablePromise.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, type EncodingError } from "./Errors.ts";
import type { AnyFunction, PayloadArgs } from "./FunctionDefinition.ts";
import { InvocationParam } from "./Invocation.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";
import type { PromiseDeclaration, PromiseFailure, PromiseSuccess } from "./PromiseDefinition.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import { Tasks } from "./Task.ts";

export interface InvocationOptions {
  readonly target?: Protocol.WorkerGroup;
  readonly timeout?: Duration.Duration;
  readonly tags?: Protocol.Tags;
  readonly version?: Protocol.FunctionVersionOrLatest;
  readonly retryPolicy?: RetryPolicy.RetryPolicy;
  readonly nonRetryableErrors?: ReadonlyArray<Schema.Codec<unknown, unknown, never, never>>;
}

export interface DurableHandle<A = unknown, E = unknown> {
  readonly id: Protocol.PromiseId;
  readonly await: Effect.Effect<A, E | DurablePromiseCanceled | DurablePromiseTimedOut | EncodingError>;
  readonly poll: Effect.Effect<Option.Option<Exit.Exit<A, E>>, unknown>;
  readonly cancel: Effect.Effect<void, unknown>;
}

export interface ResonateClientOptions {
  readonly group?: Protocol.WorkerGroup;
  readonly pid?: Protocol.ProcessId;
  readonly ttl?: Duration.Duration;
  readonly timeout?: Duration.Duration;
  readonly idPrefix?: string;
}

export interface InvocationMethods {
  <F extends AnyFunction>(options: {
    readonly targetFunction: F;
    readonly executionId: Protocol.ExecutionId;
    readonly args: PayloadArgs<F>;
    readonly options?: InvocationOptions;
  }): Effect.Effect<DurableHandle, unknown>;
  (options: {
    readonly targetFunction: string;
    readonly executionId: Protocol.ExecutionId;
    readonly args: ReadonlyArray<unknown>;
    readonly options?: InvocationOptions;
  }): Effect.Effect<DurableHandle, unknown>;
}

export interface AwaitInvocationMethods {
  <F extends AnyFunction>(options: {
    readonly targetFunction: F;
    readonly executionId: Protocol.ExecutionId;
    readonly args: PayloadArgs<F>;
    readonly options?: InvocationOptions;
  }): Effect.Effect<unknown, unknown>;
  (options: {
    readonly targetFunction: string;
    readonly executionId: Protocol.ExecutionId;
    readonly args: ReadonlyArray<unknown>;
    readonly options?: InvocationOptions;
  }): Effect.Effect<unknown, unknown>;
}

const globalScope = Schema.Literal("global").make("global");
const resolvedState = Schema.Literal("resolved").make("resolved");
const rejectedState = Schema.Literal("rejected").make("rejected");
const canceledState = Schema.Literal("rejected_canceled").make("rejected_canceled");

const prefixedId = (options: {
  readonly id: Protocol.ExecutionId;
  readonly prefix: Option.Option<string>;
}): Protocol.PromiseId =>
  Protocol.PromiseId.make(
    Option.match(options.prefix, {
      onNone: () => options.id,
      onSome: (prefix) => `${prefix}:${options.id}`,
    }),
  );

export interface ResonateClientService {
  readonly beginRun: InvocationMethods;
  readonly run: AwaitInvocationMethods;
  readonly beginRpc: InvocationMethods;
  readonly rpc: AwaitInvocationMethods;
  readonly resolve: <P extends PromiseDeclaration>(options: {
    readonly declaration: P;
    readonly id: Protocol.PromiseId;
    readonly value: PromiseSuccess<P>;
  }) => Effect.Effect<void, unknown>;
  readonly reject: <P extends PromiseDeclaration>(options: {
    readonly declaration: P;
    readonly id: Protocol.PromiseId;
    readonly error: PromiseFailure<P>;
  }) => Effect.Effect<void, unknown>;
  readonly get: <F extends AnyFunction>(options: {
    readonly fn: F;
    readonly id: Protocol.ExecutionId;
  }) => Effect.Effect<DurableHandle>;
  readonly cancel: (id: Protocol.PromiseId) => Effect.Effect<void, unknown>;
}

/**
 * Client service for starting, awaiting, resolving, and inspecting durable work.
 *
 * @category services
 * @since 0.0.0
 */
export class ResonateClient extends Context.Service<ResonateClient, ResonateClientService>()("effect-resonate/Client") {
  static layer(options?: ResonateClientOptions): Layer.Layer<ResonateClient, never, ResonateNetwork | Crypto.Crypto> {
    return Layer.effect(
      ResonateClient,
      Effect.gen(function* () {
        const promises = yield* DurablePromises;
        const tasks = yield* Tasks;
        const codec = yield* currentCodec;
        const network = yield* ResonateNetwork;
        const groupName = options?.group ?? Protocol.WorkerGroup.make("default");
        const pid = options?.pid ?? Protocol.ProcessId.make("client");
        const ttl = options?.ttl ?? Duration.seconds(60);
        const defaultTimeout = options?.timeout ?? Duration.hours(24);
        const idPrefix = Option.fromNullishOr(options?.idPrefix);

        const encodeInvocation = Effect.fn("ResonateClient.encodeInvocation")(function* (options: {
          readonly name: string;
          readonly args: ReadonlyArray<unknown>;
          readonly version: Protocol.FunctionVersionOrLatest;
          readonly retry?: RetryPolicy.RetryPolicy;
        }): Effect.fn.Return<Protocol.Value, unknown> {
          const invocation = InvocationParam.make({
            func: options.name,
            args: options.args,
            version: options.version,
            ...(Predicate.isNotUndefined(options.retry) ? { retry: options.retry } : {}),
          });
          const encoded = yield* codec.encode(invocation);
          return withSchemaHeader({ value: encoded, schemaName: options.name });
        });

        const encodeTargetPayload = Effect.fn("ResonateClient.encodeTargetPayload")(function* (options: {
          readonly target: AnyFunction | string;
          readonly args: ReadonlyArray<unknown>;
          readonly callOptions?: InvocationOptions;
        }): Effect.fn.Return<Protocol.Value, unknown> {
          if (Predicate.isString(options.target)) {
            return yield* encodeInvocation({
              name: options.target,
              args: options.args,
              version: Predicate.isUndefined(options.callOptions?.version)
                ? Protocol.FunctionVersion.make(1)
                : options.callOptions.version,
              retry: options.callOptions?.retryPolicy,
            });
          }
          const target = options.target;
          const encodedArgs = yield* Schema.encodeUnknownEffect(target.payload)(options.args).pipe(
            Effect.catchCause(() =>
              options.args.length === 1
                ? Schema.encodeUnknownEffect(target.payload)(options.args[0])
                : Effect.die("Invalid function payload"),
            ),
          );
          return yield* encodeInvocation({
            name: target.name,
            args: Arr.ensure(encodedArgs),
            version: options.callOptions?.version ?? target.version,
            retry: options.callOptions?.retryPolicy,
          });
        });

        const rootTags = (options: {
          readonly id: Protocol.PromiseId;
          readonly target: Protocol.TargetAddress;
          readonly extra: Protocol.Tags;
        }): Protocol.Tags =>
          Protocol.Tags.make({
            reserved: {
              ...options.extra.reserved,
              "resonate:origin": options.id,
              "resonate:prefix": options.id,
              "resonate:branch": options.id,
              "resonate:parent": options.id,
              "resonate:scope": globalScope,
              "resonate:target": options.target,
            },
            unrecognized: options.extra.unrecognized,
            user: options.extra.user,
          });

        const decodeSettled = Effect.fn("ResonateClient.decodeSettled")(function* (promise: Protocol.PromiseSettled) {
          return yield* Match.value(promise).pipe(
            Match.when({ state: "rejected_canceled" }, (promise) => new DurablePromiseCanceled({ id: promise.id })),
            Match.when({ state: "rejected_timedout" }, (promise) => new DurablePromiseTimedOut({ id: promise.id })),
            Match.when({ state: "resolved" }, (promise) => codec.decode(promise.value)),
            Match.when({ state: "rejected" }, (promise) =>
              codec.decode(promise.value).pipe(Effect.flatMap(Effect.fail)),
            ),
            Match.exhaustive,
          );
        });

        const handle = (id: Protocol.PromiseId): DurableHandle => ({
          id,
          await: promises.awaitSettled(id).pipe(Effect.flatMap(decodeSettled)),
          poll: promises
            .get(id)
            .pipe(
              Effect.flatMap((promise) =>
                promise.state === "pending"
                  ? Effect.succeed(Option.none())
                  : decodeSettled(promise).pipe(Effect.exit, Effect.map(Option.some)),
              ),
            ),
          cancel: promises.settle({ id, state: canceledState, value: Protocol.emptyValue }).pipe(Effect.asVoid),
        });

        const timeoutAt = Effect.fn("ResonateClient.timeoutAt")(function* (timeout: Duration.Duration) {
          const now = yield* Clock.currentTimeMillis;
          return yield* Schema.decodeUnknownEffect(Protocol.Timestamp)(now + Duration.toMillis(timeout));
        });

        const beginRpcImpl = Effect.fn("ResonateClient.beginRpc")(function* (options: {
          readonly targetFunction: AnyFunction | string;
          readonly executionId: Protocol.ExecutionId;
          readonly args: ReadonlyArray<unknown>;
          readonly options?: InvocationOptions;
        }): Effect.fn.Return<DurableHandle, unknown> {
          const id = prefixedId({ id: options.executionId, prefix: idPrefix });
          const target = network.match(options.options?.target ?? groupName);
          const param = yield* encodeTargetPayload({
            target: options.targetFunction,
            args: options.args,
            callOptions: options.options,
          });
          yield* promises.create({
            id,
            timeoutAt: yield* timeoutAt(options.options?.timeout ?? defaultTimeout),
            param,
            tags: rootTags({ id, target, extra: options.options?.tags ?? Protocol.emptyTags }),
          });
          return handle(id);
        });

        const beginRunImpl = Effect.fn("ResonateClient.beginRun")(function* (options: {
          readonly targetFunction: AnyFunction | string;
          readonly executionId: Protocol.ExecutionId;
          readonly args: ReadonlyArray<unknown>;
          readonly options?: InvocationOptions;
        }): Effect.fn.Return<DurableHandle, unknown> {
          const id = prefixedId({ id: options.executionId, prefix: idPrefix });
          const param = yield* encodeTargetPayload({
            target: options.targetFunction,
            args: options.args,
            callOptions: options.options,
          });
          yield* tasks.create({
            pid,
            ttl,
            action: Protocol.PromiseCreateRequest.make({
              head: Protocol.RequestHead.make({
                corrId: Protocol.CorrelationId.make(`${id}:create`),
                version: Protocol.protocolVersion,
              }),
              data: {
                id,
                timeoutAt: yield* timeoutAt(options.options?.timeout ?? defaultTimeout),
                param,
                tags: rootTags({
                  id,
                  target: network.anycast(groupName),
                  extra: options.options?.tags ?? Protocol.emptyTags,
                }),
              },
            }),
          });
          return handle(id);
        });

        return ResonateClient.of({
          beginRun: beginRunImpl,
          run: Effect.fn("ResonateClient.run")(function* (options: {
            readonly targetFunction: AnyFunction | string;
            readonly executionId: Protocol.ExecutionId;
            readonly args: ReadonlyArray<unknown>;
            readonly options?: InvocationOptions;
          }): Effect.fn.Return<unknown, unknown> {
            const current = yield* beginRunImpl(options);
            return yield* current.await;
          }),
          beginRpc: beginRpcImpl,
          rpc: Effect.fn("ResonateClient.rpc")(function* (options: {
            readonly targetFunction: AnyFunction | string;
            readonly executionId: Protocol.ExecutionId;
            readonly args: ReadonlyArray<unknown>;
            readonly options?: InvocationOptions;
          }): Effect.fn.Return<unknown, unknown> {
            const current = yield* beginRpcImpl(options);
            return yield* current.await;
          }),
          resolve: Effect.fn("ResonateClient.resolve")(function* (options) {
            const encoded = yield* Schema.encodeUnknownEffect(options.declaration.success)(options.value);
            const protocolValue = yield* codec.encode(encoded);
            yield* promises.settle({
              id: options.id,
              state: resolvedState,
              value: withSchemaHeader({ value: protocolValue, schemaName: options.declaration.name }),
            });
          }),
          reject: Effect.fn("ResonateClient.reject")(function* (options) {
            if (Predicate.isUndefined(options.declaration.error)) {
              return yield* Effect.die(`Promise declaration '${options.declaration.name}' has no error schema`);
            }
            const encoded = yield* Schema.encodeUnknownEffect(options.declaration.error)(options.error);
            const protocolValue = yield* codec.encode(encoded);
            yield* promises.settle({
              id: options.id,
              state: rejectedState,
              value: withSchemaHeader({ value: protocolValue, schemaName: options.declaration.name }),
            });
          }),
          get: Effect.fn("ResonateClient.get")(function* <F extends AnyFunction>(options: {
            readonly fn: F;
            readonly id: Protocol.ExecutionId;
          }): Effect.fn.Return<DurableHandle> {
            return handle(prefixedId({ id: options.id, prefix: idPrefix }));
          }),
          cancel: Effect.fn("ResonateClient.cancel")(function* (id) {
            yield* promises.settle({ id, state: canceledState, value: Protocol.emptyValue });
          }),
        });
      }),
    ).pipe(Layer.provideMerge(Layer.mergeAll(DurablePromises.layer, Tasks.layer)));
  }
}
