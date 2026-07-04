/**
 * Typed registry for durable function handlers.
 *
 * @since 0.0.0
 */
import { Array as Arr, Effect, HashSet, Option, Order, Pipeable } from "effect";
import type { AnyFunction, HandlerFunction } from "./FunctionDefinition.ts";
import * as Protocol from "./Protocol.ts";

/**
 * Registered durable function definition and implementation.
 *
 * @category models
 * @since 0.0.0
 */
export interface RegistryItem<F extends AnyFunction = AnyFunction> {
  readonly definition: F;
  readonly handler: HandlerFunction<F>;
}

const TypeId = "effect-resonate/Registry";
const ItemByVersion = Order.mapInput(Order.Number, (item: RegistryItem) => item.definition.version);

/**
 * Durable function handler registry used by workers and the execution engine.
 *
 * @category models
 * @since 0.0.0
 */
export interface Registry {
  readonly [TypeId]: typeof TypeId;
  readonly items: ReadonlyArray<RegistryItem>;
  readonly pipe: typeof Pipeable.Prototype.pipe;
  readonly get: (options: {
    readonly name: string;
    readonly version?: Protocol.FunctionVersionOrLatest;
  }) => Option.Option<RegistryItem>;
}

export namespace Registry {
  /**
   * Constructs a registry, rejecting duplicate name/version pairs.
   *
   * @category constructors
   * @since 0.0.0
   */
  export const make = (items: ReadonlyArray<RegistryItem>): Effect.Effect<Registry> => {
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
      [TypeId]: TypeId,
      items,
      get(options) {
        const version = options.version ?? "latest";
        const named = Arr.filter(items, (item) => item.definition.name === options.name);
        return Arr.match(named, {
          onEmpty: Option.none,
          onNonEmpty: (named) =>
            version !== "latest"
              ? Arr.findFirst(named, (item) => item.definition.version === version)
              : Option.some(Arr.max(named, ItemByVersion)),
        });
      },
    });
  };
}
