/**
 * Namespace entry: function, group, layers, client access.
 *
 * See `docs/DESIGN.md` §3.4 (Layer 4 — Function API) and §4 (Public API by Example).
 */
import { Clock, Context, Duration, Effect, Exit, Layer, Option, Predicate, Schema } from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import { DurablePromises } from "./DurablePromise.ts";
import { DurablePromiseCanceled, DurablePromiseTimedOut, type EncodingError } from "./Errors.ts";
import * as NetworkHttp from "./NetworkHttp.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";
import { ResonateCodec, withSchemaHeader } from "./Codec.ts";
import type { ResonateContext } from "./ResonateContext.ts";
import { Tasks } from "./Task.ts";

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

export class Registry {
  private readonly items: ReadonlyArray<RegistryItem>;

  private constructor(items: ReadonlyArray<RegistryItem>) {
    this.items = items;
  }

  static make(items: ReadonlyArray<RegistryItem>): Effect.Effect<Registry> {
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
    return Effect.succeed(new Registry(items));
  }

  get(name: string, version: Protocol.FunctionVersionOrLatest = "latest"): Option.Option<RegistryItem> {
    const named = this.items.filter((item) => item.definition.name === name);
    if (named.length === 0) {
      return Option.none();
    }
    if (version !== "latest") {
      return Option.fromNullishOr(named.find((item) => item.definition.version === version));
    }
    const latest = named.reduce((left, right) => (left.definition.version > right.definition.version ? left : right));
    return Option.some(latest);
  }
}

export class FunctionGroup<Fns extends ReadonlyArray<AnyFunction>> {
  readonly fns: Fns;

  constructor(fns: Fns) {
    this.fns = fns;
  }

  of<const Handlers extends HandlersFrom<Fns[number]>>(handlers: Handlers): Handlers {
    return handlers;
  }

  toLayer<const Handlers extends HandlersFrom<Fns[number]>, E = never, R = never>(
    build: Handlers | Effect.Effect<Handlers, E, R>,
  ): Layer.Layer<Handler<Fns[number]>, E, R> {
    return Layer.effectContext(
      (Effect.isEffect(build) ? build : Effect.succeed(build)).pipe(
        Effect.flatMap((handlers) => this.toContext(handlers)),
      ),
    );
  }

  toLayerHandler<const Name extends Fns[number]["name"], E = never, R = never>(
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
  }

  registry(): Effect.Effect<Registry, never, Handler<Fns[number]>> {
    const fns = this.fns;
    return Effect.gen(function* () {
      const items: Array<RegistryItem> = [];
      for (const definition of fns) {
        const handler = yield* Handler(definition);
        items.push({ definition, handler });
      }
      return yield* Registry.make(items);
    });
  }

  private toContext<const Handlers extends HandlersFrom<Fns[number]>>(
    handlers: Handlers,
  ): Effect.Effect<Context.Context<Handler<Fns[number]>>> {
    let context = Context.empty() as Context.Context<Handler<Fns[number]>>;
    const items: Array<RegistryItem> = [];
    for (const definition of this.fns) {
      const handler = handlers[definition.name as keyof Handlers] as HandlerFunction<typeof definition>;
      items.push({ definition, handler });
      context = Context.add(context, Handler(definition), handler);
    }
    return Effect.as(Registry.make(items), context);
  }
}

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
  new FunctionGroup(fns);

const InvocationParam = Schema.Struct({
  func: Schema.String,
  args: Schema.Array(Schema.Unknown),
  version: Protocol.FunctionVersionFromWire,
});

export interface InvocationOptions {
  readonly target?: Protocol.WorkerGroup;
  readonly timeout?: Duration.Duration;
  readonly tags?: Protocol.Tags;
  readonly version?: Protocol.FunctionVersionOrLatest;
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
const canceledState = Schema.Literal("rejected_canceled").make("rejected_canceled");

const prefixedId = (id: Protocol.ExecutionId, prefix: Option.Option<string>): Protocol.PromiseId =>
  Protocol.PromiseId.make(
    Option.match(prefix, {
      onNone: () => id,
      onSome: (prefix) => `${prefix}:${id}`,
    }),
  );

export class ResonateClient extends Context.Service<
  ResonateClient,
  {
    readonly beginRun: InvocationMethods;
    readonly run: AwaitInvocationMethods;
    readonly beginRpc: InvocationMethods;
    readonly rpc: AwaitInvocationMethods;
    readonly get: <F extends AnyFunction>(fn: F, id: Protocol.ExecutionId) => Effect.Effect<DurableHandle>;
    readonly cancel: (id: Protocol.PromiseId) => Effect.Effect<void, unknown>;
  }
>()("effect-resonate/Client") {
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
        ): Effect.fn.Return<Protocol.Value, unknown> {
          const invocation = InvocationParam.make({
            func: name,
            args,
            version,
          });
          const encoded = yield* codec.encode(invocation);
          return withSchemaHeader(encoded, name);
        });

        const encodePayload = Effect.fn("ResonateClient.encodePayload")(function* <F extends AnyFunction>(
          fn: F,
          args: ReadonlyArray<unknown>,
          version: Protocol.FunctionVersionOrLatest,
        ): Effect.fn.Return<Protocol.Value, unknown> {
          const encodedArgs = yield* Schema.encodeUnknownEffect(fn.payload)(args).pipe(
            Effect.catchCause(() =>
              args.length === 1
                ? Schema.encodeUnknownEffect(fn.payload)(args[0])
                : Effect.die("Invalid function payload"),
            ),
          );
          return yield* encodeInvocation(fn.name, Array.isArray(encodedArgs) ? encodedArgs : [encodedArgs], version);
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
            );
          }
          return yield* encodePayload(target, args, callOptions?.version ?? target.version);
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

        const beginRpc = Effect.fn("ResonateClient.beginRpc")(function* (
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

        const beginRun = Effect.fn("ResonateClient.beginRun")(function* (
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

        const get = Effect.fn("ResonateClient.get")(function* <F extends AnyFunction>(
          _fn: F,
          executionId: Protocol.ExecutionId,
        ): Effect.fn.Return<DurableHandle> {
          return handle(prefixedId(executionId, idPrefix));
        });

        const cancel = Effect.fn("ResonateClient.cancel")(function* (
          id: Protocol.PromiseId,
        ): Effect.fn.Return<void, unknown> {
          yield* promises.settle({ id, state: canceledState, value: Protocol.emptyValue });
        });

        const run = Effect.fn("ResonateClient.run")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<unknown, unknown> {
          const current = yield* beginRun(targetFunction, executionId, args, callOptions);
          return yield* current.await;
        });

        const rpc = Effect.fn("ResonateClient.rpc")(function* (
          targetFunction: AnyFunction | string,
          executionId: Protocol.ExecutionId,
          args: ReadonlyArray<unknown>,
          callOptions?: InvocationOptions,
        ): Effect.fn.Return<unknown, unknown> {
          const current = yield* beginRpc(targetFunction, executionId, args, callOptions);
          return yield* current.await;
        });

        return ResonateClient.of({
          beginRun,
          run,
          beginRpc,
          rpc,
          get,
          cancel,
        });
      }),
    );
  }
}
