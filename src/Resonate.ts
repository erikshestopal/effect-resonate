import { Clock, Context, Cron, Duration, Effect, Exit, Layer, Option, Pipeable, Predicate, Schema } from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import { DurablePromises } from "./DurablePromise.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, type EncodingError } from "./Errors.ts";
import * as NetworkHttp from "./NetworkHttp.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";
import { ResonateCodec, withSchemaHeader } from "./Codec.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import type { ResonateContext } from "./ResonateContext.ts";
import { Schedules } from "./Schedule.ts";
import { Tasks } from "./Task.ts";

export * as Worker from "./Worker.ts";

export const layerHttp = (
  options: NetworkHttp.NetworkHttpOptions,
): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> => NetworkHttp.layer(options);

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

export interface Registry {
  new (_: never): {};
  readonly [RegistryTypeId]: typeof RegistryTypeId;
  readonly items: ReadonlyArray<RegistryItem>;
  readonly pipe: typeof Pipeable.Prototype.pipe;
  readonly get: (name: string, version?: Protocol.FunctionVersionOrLatest) => Option.Option<RegistryItem>;
}

const RegistryProto = {
  ...Pipeable.Prototype,
  get(this: Registry, name: string, version: Protocol.FunctionVersionOrLatest = "latest") {
    const named = this.items.filter((item) => item.definition.name === name);
    if (named.length === 0) {
      return Option.none();
    }
    if (version !== "latest") {
      return Option.fromNullishOr(named.find((item) => item.definition.version === version));
    }
    return Option.some(
      named.reduce((left, right) => (left.definition.version > right.definition.version ? left : right)),
    );
  },
};

const makeRegistryProto = (items: ReadonlyArray<RegistryItem>): Registry =>
  Object.assign(function () {}, RegistryProto, {
    [RegistryTypeId]: RegistryTypeId,
    items,
  }) as unknown as Registry;

const makeRegistry = (items: ReadonlyArray<RegistryItem>): Effect.Effect<Registry> => {
  const seen = new Set<string>();
  for (const item of items) {
    const key = `${item.definition.name}:${item.definition.version}`;
    if (seen.has(key)) {
      return Effect.die(
        `Function '${item.definition.name}' (version ${item.definition.version}) is already registered`,
      );
    }
    seen.add(key);
  }
  return Effect.succeed(makeRegistryProto(items));
};

export interface FunctionGroup<Fns extends ReadonlyArray<AnyFunction>> {
  new (_: never): {};
  readonly [FunctionGroupTypeId]: typeof FunctionGroupTypeId;
  readonly fns: Fns;
  readonly pipe: typeof Pipeable.Prototype.pipe;
  readonly of: <const Handlers extends HandlersFrom<Fns[number]>>(handlers: Handlers) => Handlers;
  readonly toLayer: <const Handlers extends HandlersFrom<Fns[number]>, E = never, R = never>(
    build: Handlers | Effect.Effect<Handlers, E, R>,
  ) => Layer.Layer<Handler<Fns[number]>, E, R>;
  readonly toLayerHandler: <const Name extends Fns[number]["name"], E = never, R = never>(
    name: Name,
    build:
      | HandlerFunction<Extract<Fns[number], { readonly name: Name }>>
      | Effect.Effect<HandlerFunction<Extract<Fns[number], { readonly name: Name }>>, E, R>,
  ) => Layer.Layer<Handler<Extract<Fns[number], { readonly name: Name }>>, E, R>;
  readonly registry: () => Effect.Effect<Registry, never, Handler<Fns[number]>>;
}

const functionGroupToContext = <
  Fns extends ReadonlyArray<AnyFunction>,
  const Handlers extends HandlersFrom<Fns[number]>,
>(
  self: FunctionGroup<Fns>,
  handlers: Handlers,
): Effect.Effect<Context.Context<Handler<Fns[number]>>> => {
  let context = Context.empty() as Context.Context<Handler<Fns[number]>>;
  const items: Array<RegistryItem> = [];
  for (const definition of self.fns) {
    const handler = handlers[definition.name as keyof Handlers] as HandlerFunction<typeof definition>;
    items.push({ definition, handler });
    context = Context.add(context, Handler(definition), handler);
  }
  return Effect.as(makeRegistry(items), context);
};

const FunctionGroupProto = {
  ...Pipeable.Prototype,
  of: <const Handlers extends HandlersFrom<AnyFunction>>(handlers: Handlers): Handlers => handlers,
  toLayer<
    const Fns extends ReadonlyArray<AnyFunction>,
    const Handlers extends HandlersFrom<Fns[number]>,
    E = never,
    R = never,
  >(
    this: FunctionGroup<Fns>,
    build: Handlers | Effect.Effect<Handlers, E, R>,
  ): Layer.Layer<Handler<Fns[number]>, E, R> {
    return Layer.effectContext(
      (Effect.isEffect(build) ? build : Effect.succeed(build)).pipe(
        Effect.flatMap((handlers) => functionGroupToContext(this, handlers)),
      ),
    );
  },
  toLayerHandler<
    const Fns extends ReadonlyArray<AnyFunction>,
    const Name extends Fns[number]["name"],
    E = never,
    R = never,
  >(
    this: FunctionGroup<Fns>,
    name: Name,
    build:
      | HandlerFunction<Extract<Fns[number], { readonly name: Name }>>
      | Effect.Effect<HandlerFunction<Extract<Fns[number], { readonly name: Name }>>, E, R>,
  ): Layer.Layer<Handler<Extract<Fns[number], { readonly name: Name }>>, E, R> {
    const definition = this.fns.find((fn) => fn.name === name);
    if (Predicate.isUndefined(definition)) {
      return Layer.effectContext(Effect.die(`Function '${name}' is not part of this group`));
    }
    return Layer.effect(Handler(definition), Effect.isEffect(build) ? build : Effect.succeed(build));
  },

  registry<Fns extends ReadonlyArray<AnyFunction>>(
    this: FunctionGroup<Fns>,
  ): Effect.Effect<Registry, never, Handler<Fns[number]>> {
    const fns = this.fns;
    return Effect.gen(function* () {
      const items: Array<RegistryItem> = [];
      for (const definition of fns) {
        const handler = yield* Handler(definition);
        items.push({ definition, handler });
      }
      return yield* makeRegistry(items);
    });
  },
};

const makeFunctionGroupProto = <Fns extends ReadonlyArray<AnyFunction>>(fns: Fns): FunctionGroup<Fns> =>
  Object.assign(function () {}, FunctionGroupProto, {
    [FunctionGroupTypeId]: FunctionGroupTypeId,
    fns,
  }) as unknown as FunctionGroup<Fns>;

export const defineFunction = <const Name extends string, Payload extends Schema.Codec<unknown, unknown, never, never>>(
  name: Name,
  options: { readonly payload: Payload; readonly version?: number | Protocol.FunctionVersion },
): Definition<Name, Payload> => ({
  name,
  payload: options.payload,
  version: Protocol.FunctionVersion.make(options.version ?? 1),
});

export { defineFunction as function };

export const group = <const Fns extends ReadonlyArray<AnyFunction>>(...fns: Fns): FunctionGroup<Fns> =>
  makeFunctionGroupProto(fns);

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
  readonly create: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules | ResonateCodec | ResonateNetwork>;
  readonly get: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules>;
  readonly delete: Effect.Effect<void, unknown, Schedules>;
  readonly layer: Layer.Layer<never, unknown, Schedules | ResonateCodec | ResonateNetwork>;
}

const fullCronSegment = (values: ReadonlySet<number>, min: number, max: number): boolean => {
  if (values.size === 0) {
    return true;
  }
  if (values.size !== max - min + 1) {
    return false;
  }
  for (let value = min; value <= max; value = value + 1) {
    if (!values.has(value)) {
      return false;
    }
  }
  return true;
};

const cronSegment = (values: ReadonlySet<number>, min: number, max: number): string => {
  if (fullCronSegment(values, min, max)) {
    return "*";
  }
  return [...values].sort((left, right) => left - right).join(",");
};

const fiveFieldCronExpression = (cron: Cron.Cron): Effect.Effect<string, never> => {
  if (cron.seconds.size !== 1 || !cron.seconds.has(0)) {
    return Effect.die("Resonate schedules only support five-field cron expressions");
  }
  return Effect.succeed(
    [
      cronSegment(cron.minutes, 0, 59),
      cronSegment(cron.hours, 0, 23),
      cronSegment(cron.days, 1, 31),
      cronSegment(cron.months, 1, 12),
      cronSegment(cron.weekdays, 0, 6),
    ].join(" "),
  );
};

export const schedule = <F extends AnyFunction>(options: ScheduleOptions<F>): ScheduleValue<F> => {
  const timeout = options.timeout ?? Duration.hours(24);
  const tags = options.tags ?? Protocol.emptyTags;
  const version = options.version ?? options.function.version;
  const retry = options.retryPolicy;

  const create: ScheduleValue<F>["create"] = Effect.gen(function* () {
    const schedules = yield* Schedules;
    const codec = yield* ResonateCodec;
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
      promiseParam: withSchemaHeader(encoded, options.function.name),
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

export function promise<const Name extends string, Success extends Schema.Codec<unknown, unknown, never, never>>(
  name: Name,
  options: { readonly success: Success },
): PromiseDeclaration<Name, Success, undefined>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(
  name: Name,
  options: { readonly success: Success; readonly error: Failure },
): PromiseDeclaration<Name, Success, Failure>;
export function promise<
  const Name extends string,
  Success extends Schema.Codec<unknown, unknown, never, never>,
  Failure extends Schema.Codec<unknown, unknown, never, never>,
>(name: Name, options: { readonly success: Success; readonly error?: Failure }) {
  return {
    name,
    success: options.success,
    error: options.error,
    id: (executionId: Protocol.ExecutionId | Protocol.PromiseId) => Protocol.PromiseId.make(`${executionId}.${name}`),
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
  <F extends AnyFunction>(
    fn: F,
    id: Protocol.ExecutionId,
    args: PayloadArgs<F>,
    options?: InvocationOptions,
  ): Effect.Effect<DurableHandle, unknown>;
  (
    name: string,
    id: Protocol.ExecutionId,
    args: ReadonlyArray<unknown>,
    options?: InvocationOptions,
  ): Effect.Effect<DurableHandle, unknown>;
}

export interface AwaitInvocationMethods {
  <F extends AnyFunction>(
    fn: F,
    id: Protocol.ExecutionId,
    args: PayloadArgs<F>,
    options?: InvocationOptions,
  ): Effect.Effect<unknown, unknown>;
  (
    name: string,
    id: Protocol.ExecutionId,
    args: ReadonlyArray<unknown>,
    options?: InvocationOptions,
  ): Effect.Effect<unknown, unknown>;
}

const globalScope = Schema.Literal("global").make("global");
const resolvedState = Schema.Literal("resolved").make("resolved");
const rejectedState = Schema.Literal("rejected").make("rejected");
const canceledState = Schema.Literal("rejected_canceled").make("rejected_canceled");

const prefixedId = (id: Protocol.ExecutionId, prefix: Option.Option<string>): Protocol.PromiseId =>
  Protocol.PromiseId.make(
    Option.match(prefix, {
      onNone: () => id,
      onSome: (prefix) => `${prefix}:${id}`,
    }),
  );

export interface ResonateClientService {
  readonly beginRun: InvocationMethods;
  readonly run: AwaitInvocationMethods;
  readonly beginRpc: InvocationMethods;
  readonly rpc: AwaitInvocationMethods;
  readonly resolve: <P extends PromiseDeclaration>(
    declaration: P,
    id: Protocol.PromiseId,
    value: PromiseSuccess<P>,
  ) => Effect.Effect<void, unknown>;
  readonly reject: <P extends PromiseDeclaration>(
    declaration: P,
    id: Protocol.PromiseId,
    error: PromiseFailure<P>,
  ) => Effect.Effect<void, unknown>;
  readonly get: <F extends AnyFunction>(fn: F, id: Protocol.ExecutionId) => Effect.Effect<DurableHandle>;
  readonly cancel: (id: Protocol.PromiseId) => Effect.Effect<void, unknown>;
}

export class ResonateClient extends Context.Service<ResonateClient, ResonateClientService>()("effect-resonate/Client") {
  static layer(
    options?: ResonateClientOptions,
  ): Layer.Layer<ResonateClient, never, DurablePromises | Tasks | ResonateCodec | ResonateNetwork> {
    return Layer.effect(
      ResonateClient,
      Effect.gen(function* () {
        const promises = yield* DurablePromises;
        const tasks = yield* Tasks;
        const codec = yield* ResonateCodec;
        const network = yield* ResonateNetwork;
        const groupName = options?.group ?? Protocol.WorkerGroup.make("default");
        const pid = options?.pid ?? Protocol.ProcessId.make("client");
        const ttl = options?.ttl ?? Duration.seconds(60);
        const defaultTimeout = options?.timeout ?? Duration.hours(24);
        const idPrefix = Option.fromNullishOr(options?.idPrefix);

        const encodeInvocation = Effect.fn("ResonateClient.encodeInvocation")(function* (
          name: string,
          args: ReadonlyArray<unknown>,
          version: Protocol.FunctionVersionOrLatest,
          retry?: RetryPolicy.RetryPolicy,
        ): Effect.fn.Return<Protocol.Value, unknown> {
          const invocation = InvocationParam.make({
            func: name,
            args,
            version,
            ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
          });
          const encoded = yield* codec.encode(invocation);
          return withSchemaHeader(encoded, name);
        });

        const encodeTargetPayload = Effect.fn("ResonateClient.encodeTargetPayload")(function* (
          target: AnyFunction | string,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<Protocol.Value, unknown> {
          if (Predicate.isString(target)) {
            return yield* encodeInvocation(
              target,
              args,
              Predicate.isUndefined(callOptions?.version) ? Protocol.FunctionVersion.make(1) : callOptions.version,
              callOptions?.retryPolicy,
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
            callOptions?.version ?? target.version,
            callOptions?.retryPolicy,
          );
        });

        const rootTags = (
          id: Protocol.PromiseId,
          target: Protocol.TargetAddress,
          extra: Protocol.Tags,
        ): Protocol.Tags =>
          Protocol.Tags.make({
            reserved: {
              ...extra.reserved,
              "resonate:origin": id,
              "resonate:prefix": id,
              "resonate:branch": id,
              "resonate:parent": id,
              "resonate:scope": globalScope,
              "resonate:target": target,
            },
            unrecognized: extra.unrecognized,
            user: extra.user,
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
          return Schema.decodeUnknownSync(Protocol.Timestamp)(now + Duration.toMillis(timeout));
        });

        const beginRpcImpl = Effect.fn("ResonateClient.beginRpc")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<DurableHandle, unknown> {
          const id = prefixedId(executionId, idPrefix);
          const target = network.match(callOptions?.target ?? groupName);
          const param = yield* encodeTargetPayload(targetFunction, args, callOptions);
          yield* promises.create({
            id,
            timeoutAt: yield* timeoutAt(callOptions?.timeout ?? defaultTimeout),
            param,
            tags: rootTags(id, target, callOptions?.tags ?? Protocol.emptyTags),
          });
          return handle(id);
        });
        const beginRpc: ResonateClientService["beginRpc"] = beginRpcImpl;

        const beginRunImpl = Effect.fn("ResonateClient.beginRun")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<DurableHandle, unknown> {
          const id = prefixedId(executionId, idPrefix);
          const param = yield* encodeTargetPayload(targetFunction, args, callOptions);
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
                timeoutAt: yield* timeoutAt(callOptions?.timeout ?? defaultTimeout),
                param,
                tags: rootTags(id, network.anycast(groupName), callOptions?.tags ?? Protocol.emptyTags),
              },
            }),
          });
          return handle(id);
        });
        const beginRun: ResonateClientService["beginRun"] = beginRunImpl;

        const get: ResonateClientService["get"] = Effect.fn("ResonateClient.get")(function* <F extends AnyFunction>(
          _fn: F,
          executionId: Protocol.ExecutionId,
        ): Effect.fn.Return<DurableHandle> {
          return handle(prefixedId(executionId, idPrefix));
        });

        const cancel: ResonateClientService["cancel"] = Effect.fn("ResonateClient.cancel")(function* (id) {
          yield* promises.settle({ id, state: canceledState, value: Protocol.emptyValue });
        });

        const resolve: ResonateClientService["resolve"] = Effect.fn("ResonateClient.resolve")(
          function* (declaration, id, value) {
            const encoded = yield* Schema.encodeUnknownEffect(declaration.success)(value);
            const protocolValue = yield* codec.encode(encoded);
            yield* promises.settle({
              id,
              state: resolvedState,
              value: withSchemaHeader(protocolValue, declaration.name),
            });
          },
        );

        const reject: ResonateClientService["reject"] = Effect.fn("ResonateClient.reject")(
          function* (declaration, id, error) {
            if (Predicate.isUndefined(declaration.error)) {
              return yield* Effect.die(`Promise declaration '${declaration.name}' has no error schema`);
            }
            const encoded = yield* Schema.encodeUnknownEffect(declaration.error)(error);
            const protocolValue = yield* codec.encode(encoded);
            yield* promises.settle({
              id,
              state: rejectedState,
              value: withSchemaHeader(protocolValue, declaration.name),
            });
          },
        );

        const run: ResonateClientService["run"] = Effect.fn("ResonateClient.run")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<unknown, unknown> {
          const current = yield* beginRunImpl(targetFunction, executionId, args, callOptions);
          return yield* current.await;
        });

        const rpc: ResonateClientService["rpc"] = Effect.fn("ResonateClient.rpc")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<unknown, unknown> {
          const current = yield* beginRpcImpl(targetFunction, executionId, args, callOptions);
          return yield* current.await;
        });

        return ResonateClient.of({
          beginRun,
          run,
          beginRpc,
          rpc,
          resolve,
          reject,
          get,
          cancel,
        });
      }),
    );
  }
}
