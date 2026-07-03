/**
 * High-level API for defining and invoking durable Resonate functions.
 *
 * This module contains the public authoring surface: define typed function
 * declarations with {@link function}, group them into handler registries with
 * {@link group}, create schedules and external promises, and use
 * {@link ResonateClient} to start, await, resolve, reject, or cancel durable
 * executions.
 *
 * @example
 * ```ts
 * import { Effect, Schema } from "effect"
 * import { Resonate } from "effect-resonate"
 *
 * const greet = Resonate.function({
 *   name: "greet",
 *   payload: Schema.String
 * })
 *
 * const App = Resonate.group(greet)
 *
 * const handlers = App.toLayer(
 *   App.of({
 *     greet: (name) => Effect.succeed(`Hello, ${name}!`)
 *   })
 * )
 * ```
 *
 * @since 0.0.0
 */
import {
  Array as Arr,
  Clock,
  Context,
  Cron,
  Crypto,
  Duration,
  Effect,
  Exit,
  HashSet,
  Layer,
  Option,
  Order,
  Pipeable,
  Predicate,
  Schema,
} from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import { DurablePromises } from "./DurablePromise.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, type EncodingError } from "./Errors.ts";
import * as NetworkHttp from "./network/http.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";
import { currentCodec, withSchemaHeader } from "./Codec.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import type { ResonateContext } from "./ResonateContext.ts";
import { Schedules } from "./Schedule.ts";
import { Tasks } from "./Task.ts";

export * as Worker from "./Worker.ts";

/**
 * Builds the HTTP network layer for a Resonate server.
 *
 * The layer requires an Effect `HttpClient` implementation from the application
 * runtime, such as Bun, Node, or another platform package.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerHttp = (
  options: NetworkHttp.NetworkHttpOptions,
): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> => NetworkHttp.layer(options);

/**
 * Describes a durable function name, payload schema, and version.
 *
 * @category models
 * @since 0.0.0
 */
export interface Definition<Name extends string, Payload extends Schema.Codec<unknown, unknown, never, never>> {
  readonly name: Name;
  readonly payload: Payload;
  readonly version: Protocol.FunctionVersion;
}

export type AnyFunction = Definition<string, Schema.Codec<unknown, unknown, never, never>>;

export type PayloadArgs<F extends AnyFunction> =
  F["payload"]["Type"] extends ReadonlyArray<unknown> ? F["payload"]["Type"] : readonly [F["payload"]["Type"]];

export type HandlerFunction<F extends AnyFunction> = (
  ...args: PayloadArgs<F>
) => Effect.Effect<unknown, unknown, ResonateContext>;

export interface Handler<F extends AnyFunction> {
  readonly definition: F;
}

export const Handler = <F extends AnyFunction>(definition: F): Context.Service<Handler<F>, HandlerFunction<F>> =>
  Context.Service<Handler<F>, HandlerFunction<F>>(`effect-resonate/Handler/${definition.name}/${definition.version}`);

export type HandlersFrom<F extends AnyFunction> = {
  readonly [Current in F as Current["name"]]: HandlerFunction<Current>;
};

export interface RegistryItem<F extends AnyFunction = AnyFunction> {
  readonly definition: F;
  readonly handler: HandlerFunction<F>;
}

const RegistryTypeId = "effect-resonate/Registry";
const FunctionGroupTypeId = "effect-resonate/FunctionGroup";
const RegistryItemByVersion = Order.mapInput(Order.Number, (item: RegistryItem) => item.definition.version);

export interface Registry {
  readonly [RegistryTypeId]: typeof RegistryTypeId;
  readonly items: ReadonlyArray<RegistryItem>;
  readonly pipe: typeof Pipeable.Prototype.pipe;
  readonly get: (options: {
    readonly name: string;
    readonly version?: Protocol.FunctionVersionOrLatest;
  }) => Option.Option<RegistryItem>;
}

export const makeRegistry = (items: ReadonlyArray<RegistryItem>): Effect.Effect<Registry> => {
  let seen = HashSet.empty<string>();
  for (const item of items) {
    const key = `${item.definition.name}:${item.definition.version}`;
    if (HashSet.has(seen, key)) {
      return Effect.die(
        `Function '${item.definition.name}' (version ${item.definition.version}) is already registered`,
      );
    }
    seen = HashSet.add(seen, key);
  }

  return Effect.succeed({
    ...Pipeable.Prototype,
    [RegistryTypeId]: RegistryTypeId,
    items,
    get(options) {
      const version = options.version ?? "latest";
      const named = Arr.filter(items, (item) => item.definition.name === options.name);
      return Arr.match(named, {
        onEmpty: Option.none,
        onNonEmpty: (named) =>
          version !== "latest"
            ? Arr.findFirst(named, (item) => item.definition.version === version)
            : Option.some(Arr.max(named, RegistryItemByVersion)),
      });
    },
  });
};

export interface FunctionGroup<Fns extends ReadonlyArray<AnyFunction>> {
  readonly [FunctionGroupTypeId]: typeof FunctionGroupTypeId;
  readonly fns: Fns;
  readonly pipe: typeof Pipeable.Prototype.pipe;
  readonly of: <const Handlers extends HandlersFrom<Fns[number]>>(handlers: Handlers) => Handlers;
  readonly toLayer: <const Handlers extends HandlersFrom<Fns[number]>, E = never, R = never>(
    build: Handlers | Effect.Effect<Handlers, E, R>,
  ) => Layer.Layer<Handler<Fns[number]>, E, R>;
  readonly toLayerHandler: <const Name extends Fns[number]["name"], E = never, R = never>(options: {
    readonly name: Name;
    readonly build:
      | HandlerFunction<Extract<Fns[number], { readonly name: Name }>>
      | Effect.Effect<HandlerFunction<Extract<Fns[number], { readonly name: Name }>>, E, R>;
  }) => Layer.Layer<Handler<Extract<Fns[number], { readonly name: Name }>>, E, R>;
  readonly registry: Effect.Effect<Registry, never, Handler<Fns[number]>>;
}

const functionGroupToContext = <
  Fns extends ReadonlyArray<AnyFunction>,
  const Handlers extends HandlersFrom<Fns[number]>,
>(options: {
  readonly self: FunctionGroup<Fns>;
  readonly handlers: Handlers;
}): Effect.Effect<Context.Context<Handler<Fns[number]>>> => {
  let context = Context.empty() as Context.Context<Handler<Fns[number]>>;
  const items: Array<RegistryItem> = [];
  for (const definition of options.self.fns) {
    const handler = options.handlers[definition.name as keyof Handlers] as HandlerFunction<typeof definition>;
    items.push({ definition, handler });
    context = Context.add(context, Handler(definition), handler);
  }
  return Effect.as(makeRegistry(items), context);
};

/**
 * Defines a versioned durable function and its argument schema.
 *
 * @example
 * ```ts
 * import { Schema } from "effect"
 * import { Resonate } from "effect-resonate"
 *
 * const chargeCard = Resonate.function({
 *   name: "chargeCard",
 *   payload: Schema.Struct({ orderId: Schema.String })
 * })
 * ```
 *
 * @category constructors
 * @since 0.0.0
 */
export const defineFunction = <
  const Name extends string,
  Payload extends Schema.Codec<unknown, unknown, never, never>,
>(options: {
  readonly name: Name;
  readonly payload: Payload;
  readonly version?: number | Protocol.FunctionVersion;
}): Definition<Name, Payload> => ({
  name: options.name,
  payload: options.payload,
  version: Protocol.FunctionVersion.make(options.version ?? 1),
});

export { defineFunction as function };

/**
 * Groups durable function definitions into a typed handler registry.
 *
 * @category constructors
 * @since 0.0.0
 */
export const group = <const Fns extends ReadonlyArray<AnyFunction>>(...fns: Fns): FunctionGroup<Fns> => ({
  ...Pipeable.Prototype,
  [FunctionGroupTypeId]: FunctionGroupTypeId,
  fns,
  of: (handlers) => handlers,
  toLayer(build) {
    return Layer.effectContext(
      (Effect.isEffect(build) ? build : Effect.succeed(build)).pipe(
        Effect.flatMap((handlers) => functionGroupToContext({ self: this, handlers })),
      ),
    );
  },
  toLayerHandler(options) {
    return Option.match(
      Arr.findFirst(fns, (fn) => fn.name === options.name),
      {
        onNone: () => Layer.effectContext(Effect.die(`Function '${options.name}' is not part of this group`)),
        onSome: (definition) =>
          Layer.effect(
            Handler(definition),
            Effect.isEffect(options.build) ? options.build : Effect.succeed(options.build),
          ),
      },
    );
  },
  registry: Effect.gen(function* () {
    const items: Array<RegistryItem> = [];
    for (const definition of fns) {
      const handler = yield* Handler(definition);
      items.push({ definition, handler });
    }
    return yield* makeRegistry(items);
  }),
});

export interface ScheduleOptions<F extends AnyFunction> {
  readonly id: Protocol.ScheduleId;
  readonly cron: Cron.Cron;
  readonly function: F;
  readonly payload: PayloadArgs<F>;
  readonly timeout?: Duration.Duration;
  readonly target?: Protocol.WorkerGroup;
  readonly tags?: Protocol.Tags;
  readonly version?: Protocol.FunctionVersionOrLatest;
  readonly retryPolicy?: RetryPolicy.RetryPolicy;
}

export interface ScheduleValue<F extends AnyFunction = AnyFunction> {
  readonly id: Protocol.ScheduleId;
  readonly cron: Cron.Cron;
  readonly definition: F;
  readonly payload: PayloadArgs<F>;
  readonly create: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules | ResonateNetwork>;
  readonly get: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules>;
  readonly delete: Effect.Effect<void, unknown, Schedules>;
  readonly layer: Layer.Layer<never, unknown, Schedules | ResonateNetwork>;
}

const fullCronSegment = (options: {
  readonly values: ReadonlySet<number>;
  readonly min: number;
  readonly max: number;
}): boolean => {
  if (options.values.size === 0) {
    return true;
  }
  if (options.values.size !== options.max - options.min + 1) {
    return false;
  }
  return Arr.every(Arr.range(options.min, options.max), (value) => options.values.has(value));
};

const cronSegment = (options: {
  readonly values: ReadonlySet<number>;
  readonly min: number;
  readonly max: number;
}): string => {
  if (fullCronSegment(options)) {
    return "*";
  }
  return Arr.sort(options.values, Order.Number).join(",");
};

const fiveFieldCronExpression = (cron: Cron.Cron): Effect.Effect<string, never> => {
  if (cron.seconds.size !== 1 || !cron.seconds.has(0)) {
    return Effect.die("Resonate schedules only support five-field cron expressions");
  }
  return Effect.succeed(
    [
      cronSegment({ values: cron.minutes, min: 0, max: 59 }),
      cronSegment({ values: cron.hours, min: 0, max: 23 }),
      cronSegment({ values: cron.days, min: 1, max: 31 }),
      cronSegment({ values: cron.months, min: 1, max: 12 }),
      cronSegment({ values: cron.weekdays, min: 0, max: 6 }),
    ].join(" "),
  );
};

/**
 * Defines a durable schedule for invoking a function on a cron expression.
 *
 * @category constructors
 * @since 0.0.0
 */
export const schedule = <F extends AnyFunction>(options: ScheduleOptions<F>): ScheduleValue<F> => {
  const timeout = options.timeout ?? Duration.hours(24);
  const tags = options.tags ?? Protocol.emptyTags;
  const version = options.version ?? options.function.version;
  const retry = options.retryPolicy;

  const create: ScheduleValue<F>["create"] = Effect.gen(function* () {
    const schedules = yield* Schedules;
    const codec = yield* currentCodec;
    const network = yield* ResonateNetwork;
    const encodedArgs = yield* Schema.encodeUnknownEffect(options.function.payload)(options.payload).pipe(
      Effect.catchCause(() =>
        options.payload.length === 1
          ? Schema.encodeUnknownEffect(options.function.payload)(options.payload[0])
          : Effect.die("Invalid function payload"),
      ),
    );
    const encoded = yield* codec.encode(
      InvocationParam.make({
        func: options.function.name,
        args: Array.isArray(encodedArgs) ? encodedArgs : [encodedArgs],
        version,
        ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
      }),
    );
    const target = network.match(options.target ?? Protocol.WorkerGroup.make("default"));
    return yield* schedules.create({
      id: options.id,
      cron: yield* fiveFieldCronExpression(options.cron),
      promiseId: "{{.id}}.{{.timestamp}}",
      promiseTimeout: timeout,
      promiseParam: withSchemaHeader({ value: encoded, schemaName: options.function.name }),
      promiseTags: Protocol.Tags.make({
        reserved: {
          ...tags.reserved,
          "resonate:target": target,
          "resonate:scope": globalScope,
        },
        unrecognized: tags.unrecognized,
        user: tags.user,
      }),
    });
  });

  const value = {
    id: options.id,
    cron: options.cron,
    definition: options.function,
    payload: options.payload,
    create,
    get: Schedules.pipe(Effect.flatMap((schedules) => schedules.get(options.id))),
    delete: Schedules.pipe(Effect.flatMap((schedules) => schedules.delete(options.id))),
    layer: Layer.effectDiscard(create),
  };
  return value;
};

const InvocationParam = Schema.Struct({
  func: Schema.String,
  args: Schema.Array(Schema.Unknown),
  version: Protocol.FunctionVersionFromWire,
  retry: Schema.optionalKey(RetryPolicy.RetryPolicyFromWire),
});

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

export interface PromiseDeclaration<
  Name extends string = string,
  Success extends Schema.Codec<unknown, unknown, never, never> = Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never> | undefined =
    | Schema.Codec<unknown, unknown, never, never>
    | undefined,
> {
  readonly name: Name;
  readonly success: Success;
  readonly error: Failure;
  readonly id: (executionId: Protocol.ExecutionId | Protocol.PromiseId) => Protocol.PromiseId;
}

export type PromiseSuccess<P extends PromiseDeclaration> = P["success"]["Type"];
export type PromiseFailure<P extends PromiseDeclaration> =
  P["error"] extends Schema.Codec<unknown, unknown, never, never> ? P["error"]["Type"] : never;

/**
 * Defines an externally resolvable durable promise.
 *
 * @category constructors
 * @since 0.0.0
 */
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
>(options: { readonly name: Name; readonly success: Success }): PromiseDeclaration<Name, Success, undefined>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(options: {
  readonly name: Name;
  readonly success: Success;
  readonly error: Failure;
}): PromiseDeclaration<Name, Success, Failure>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(options: { readonly name: Name; readonly success: Success; readonly error?: Failure }) {
  return {
    name: options.name,
    success: options.success,
    error: options.error,
    id: (executionId: Protocol.ExecutionId | Protocol.PromiseId) =>
      Protocol.PromiseId.make(`${executionId}.${options.name}`),
  };
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
            args: Array.isArray(encodedArgs) ? encodedArgs : [encodedArgs],
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
