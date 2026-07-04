/**
 * Durable function definitions and typed handler groups.
 *
 * @since 0.0.0
 */
import { Array as Arr, Context, Effect, Layer, Option, Pipeable, Schema } from "effect";
import * as Protocol from "./Protocol.ts";
import { Registry, type RegistryItem } from "./Registry.ts";
import type { ResonateContext } from "./ResonateContext.ts";
import * as RetryPolicy from "./RetryPolicy.ts";

/**
 * Encoded durable function invocation payload.
 *
 * @category schemas
 * @since 0.0.0
 */
export const InvocationParam = Schema.Struct({
  func: Schema.String,
  args: Schema.Array(Schema.Unknown),
  retry: Schema.optionalKey(RetryPolicy.RetryPolicyFromWire),
  version: Protocol.FunctionVersionFromWire,
});

/**
 * Encoded local durable step payload.
 *
 * Native local calls record the function name and version but not arguments.
 *
 * @category schemas
 * @since 0.0.0
 */
export const LocalInvocationParam = Schema.Struct({
  func: Schema.String,
  version: Protocol.FunctionVersionFromWire,
});

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

const FunctionGroupTypeId = "effect-resonate/FunctionGroup";

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
  return Effect.as(Registry.make(items), context);
};

/**
 * Defines a versioned durable function and its argument schema.
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
    return yield* Registry.make(items);
  }),
});
