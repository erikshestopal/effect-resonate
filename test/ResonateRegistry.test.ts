import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect, Exit, Layer, Option, Ref, Schema } from "effect";
import * as Resonate from "../src/Resonate.ts";
import { ResonateContext } from "../src/ResonateContext.ts";
import * as Protocol from "../src/Protocol.ts";

const Countdown = Resonate.function({ name: "Countdown", payload: Schema.Tuple([Schema.Number, Schema.Number]) });

const Checkout = Resonate.function({ name: "Checkout", payload: Schema.Struct({ id: Schema.String }), version: 2 });

const contextLayer = Layer.succeed(
  ResonateContext,
  ResonateContext.of({
    info: {
      attempt: 0,
      id: Protocol.PromiseId.make("test"),
      originId: Protocol.PromiseId.make("test"),
      prefixId: Protocol.PromiseId.make("test"),
      parentId: Protocol.PromiseId.make("test"),
      branchId: Protocol.PromiseId.make("test"),
      timeoutAt: Schema.decodeUnknownSync(Protocol.Timestamp)(60_000),
      version: Protocol.TaskVersion.make(1),
    },
    run: Effect.succeed,
    beginRun: (effect) =>
      Effect.succeed({
        id: Protocol.PromiseId.make("test.0"),
        await: Effect.void,
        poll: Effect.succeedNone,
        cancel: Effect.void,
      }),
    all: (effects) => Effect.forEach(effects, (effect) => effect),
    now: Effect.succeed(DateTime.makeUnsafe(0)),
    random: Effect.succeed(0),
    sleep: () => Effect.void,
    sleepUntil: () => Effect.void,
    beginRpc: () =>
      Effect.succeed({
        id: Protocol.PromiseId.make("test.1"),
        await: Effect.void,
        poll: Effect.succeedNone,
        cancel: Effect.void,
      }),
    rpc: () => Effect.void,
    detached: () =>
      Effect.succeed({
        id: Protocol.PromiseId.make("test.2"),
        await: Effect.void,
        poll: Effect.succeedNone,
        cancel: Effect.void,
      }),
    promise: () =>
      Effect.succeed({
        id: Protocol.PromiseId.make("test.3"),
        await: Effect.void,
        poll: Effect.succeedNone,
        cancel: Effect.void,
      }),
    panic: (message) => Effect.die(message),
  }),
);

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
      expect(yield* countdown(2, 3).pipe(Effect.provide(contextLayer))).toBe(5);

      const checkout = yield* Resonate.Handler(Checkout).pipe(Effect.provide(layer));
      expect(yield* checkout({ id: "order-1" }).pipe(Effect.provide(contextLayer))).toBe("order-1");

      const registry = yield* group.registry.pipe(Effect.provide(layer));
      const latest = registry.get({ name: "Checkout" });
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
      expect(yield* countdown(1, 2).pipe(Effect.provide(contextLayer))).toBe(3);
      expect(yield* countdown(3, 4).pipe(Effect.provide(contextLayer))).toBe(10);
    }),
  );

  it.effect("rejects duplicate name and version at layer build", () =>
    Effect.gen(function* () {
      const duplicate = Resonate.function({ name: "Countdown", payload: Schema.Tuple([Schema.Number, Schema.Number]) });
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
      const layer = group.toLayerHandler({ name: "Checkout", build: (order) => Effect.succeed(order.id) });
      const checkout = yield* Resonate.Handler(Checkout).pipe(Effect.provide(layer));
      expect(yield* checkout({ id: "single" }).pipe(Effect.provide(contextLayer))).toBe("single");
    }),
  );
});
