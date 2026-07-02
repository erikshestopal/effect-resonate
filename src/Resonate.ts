/**
 * Namespace entry: function, group, layers, client access.
 *
 * See `docs/DESIGN.md` §3.4 (Layer 4 — Function API) and §4 (Public API by Example).
 */
import { Context, Effect, Layer, Option, Predicate, Schema } from "effect";
import type * as HttpClient from "effect/unstable/http/HttpClient";
import * as NetworkHttp from "./NetworkHttp.ts";
import type { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";

export const layerHttp = (
  options: NetworkHttp.NetworkHttpOptions,
): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> => NetworkHttp.layer(options);

export interface Definition<Name extends string, Payload extends Schema.Schema<unknown>> {
  readonly name: Name;
  readonly payload: Payload;
  readonly version: Protocol.FunctionVersion;
}

export type AnyFunction = Definition<string, Schema.Schema<unknown>>;

export type PayloadArgs<F extends AnyFunction> =
  F["payload"]["Type"] extends ReadonlyArray<unknown> ? F["payload"]["Type"] : readonly [F["payload"]["Type"]];

export type HandlerFunction<F extends AnyFunction> = (...args: PayloadArgs<F>) => Effect.Effect<unknown, unknown>;

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

export const defineFunction = <const Name extends string, Payload extends Schema.Schema<unknown>>(
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
