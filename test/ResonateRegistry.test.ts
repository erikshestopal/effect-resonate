import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Option, Ref, Schema } from "effect";
import * as Resonate from "../src/Resonate.ts";

const Countdown = Resonate.function("Countdown", {
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
});

const Checkout = Resonate.function("Checkout", {
  payload: Schema.Struct({ id: Schema.String }),
  version: 2,
});

describe("Resonate function registry", () => {
  it.effect("builds handler layers and resolves latest registered versions", () =>
    Effect.gen(function* () {
      const group = Resonate.group(Countdown, Checkout);
      const layer = group.toLayer(
        group.of({
          Countdown: (count, delay) => Effect.succeed(count + delay),
          Checkout: (order) => Effect.succeed(order.id),
        }),
      );

      const countdown = yield* Resonate.Handler(Countdown).pipe(Effect.provide(layer));
      expect(yield* countdown(2, 3)).toBe(5);

      const checkout = yield* Resonate.Handler(Checkout).pipe(Effect.provide(layer));
      expect(yield* checkout({ id: "order-1" })).toBe("order-1");

      const registry = yield* group.registry().pipe(Effect.provide(layer));
      const latest = registry.get("Checkout");
      expect(Option.isSome(latest)).toBe(true);
      if (Option.isSome(latest)) {
        expect(latest.value.definition.version).toBe(2);
      }
    }),
  );

  it.effect("supports Effect-built handler maps with shared setup state", () =>
    Effect.gen(function* () {
      const group = Resonate.group(Countdown);
      const layer = group.toLayer(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          return group.of({
            Countdown: (count, delay) => Ref.updateAndGet(calls, (value) => value + count + delay),
          });
        }),
      );

      const countdown = yield* Resonate.Handler(Countdown).pipe(Effect.provide(layer));
      expect(yield* countdown(1, 2)).toBe(3);
      expect(yield* countdown(3, 4)).toBe(10);
    }),
  );

  it.effect("rejects duplicate name and version at layer build", () =>
    Effect.gen(function* () {
      const duplicate = Resonate.function("Countdown", {
        payload: Schema.Tuple([Schema.Number, Schema.Number]),
      });
      const group = Resonate.group(Countdown, duplicate);
      const layer = group.toLayer(
        group.of({
          Countdown: (count, delay) => Effect.succeed(count + delay),
        }),
      );
      const exit = yield* Layer.build(layer).pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("builds a single handler layer", () =>
    Effect.gen(function* () {
      const group = Resonate.group(Checkout);
      const layer = group.toLayerHandler("Checkout", (order) => Effect.succeed(order.id));
      const checkout = yield* Resonate.Handler(Checkout).pipe(Effect.provide(layer));
      expect(yield* checkout({ id: "single" })).toBe("single");
    }),
  );
});
