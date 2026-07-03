import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { DateTime, Duration, Effect, Exit, Layer, Option, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { currentCodec, ResonateCodec } from "../src/Codec.ts";
import { DurablePromises } from "../src/DurablePromise.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ResonateContext } from "../src/ResonateContext.ts";
import { assertInvariants, ResonateTest, restartWorker, snapshot } from "../src/testing.ts";

const DstCounter = Resonate.function({ name: "DstCounter", payload: Schema.Number });

const DstFanout = Resonate.function({ name: "DstFanout", payload: Schema.Number });

const DstSleep = Resonate.function({ name: "DstSleep", payload: Schema.Number });

const DstFns = Resonate.group(DstCounter, DstFanout, DstSleep);

interface Prng {
  readonly next: Effect.Effect<number>;
  readonly between: (maxExclusive: number) => Effect.Effect<number>;
  readonly chance: (probability: number) => Effect.Effect<boolean>;
}

const makePrng = Effect.fn("makePrng")(function* (seed: number) {
  const state = yield* Ref.make(seed >>> 0);
  const next = Ref.modify(state, (current) => {
    const updated = (Math.imul(current, 1664525) + 1013904223) >>> 0;
    return [updated / 0x100000000, updated];
  });
  return {
    next,
    between: (maxExclusive: number) => next.pipe(Effect.map((value) => Math.floor(value * maxExclusive))),
    chance: (probability: number) => next.pipe(Effect.map((value) => value < probability)),
  };
});

const perturb = Effect.fn("DstSimulator.perturb")(function* (prng: Prng) {
  if (yield* prng.chance(0.35)) {
    yield* restartWorker;
  }
  const yields = yield* prng.between(4);
  for (let index = 0; index <= yields; index = index + 1) {
    yield* Effect.yieldNow;
  }
  if (yield* prng.chance(0.7)) {
    yield* TestClock.adjust(Duration.seconds(30 + (yield* prng.between(90))));
  }
  yield* snapshot.pipe(Effect.flatMap(assertInvariants));
});

const awaitResolved = Effect.fn("DstSimulator.awaitResolved")(function* (id: Protocol.PromiseId, seed: number) {
  const promises = yield* DurablePromises;
  const codec = yield* currentCodec;
  const promise = yield* promises.get(id);
  if (promise.state !== "resolved") {
    return yield* Effect.die(`DST seed ${seed} left '${id}' in state '${promise.state}'`);
  }
  return yield* codec.decode(promise.value);
});

const runProgramCorpus = Effect.fn("DstSimulator.runProgramCorpus")(function* (seed: number) {
  const prng = yield* makePrng(seed);
  const sideEffects = yield* Ref.make(0);
  const handlers = DstFns.toLayer(
    DstFns.of({
      DstCounter: (value) =>
        Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
          const ctx = yield* ResonateContext;
          yield* ctx.run({ effect: Ref.update(sideEffects, (count) => count + 1) });
          yield* ctx.sleep(Duration.seconds(60));
          return Number(value) + 1;
        }),
      DstFanout: (value) =>
        Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
          const ctx = yield* ResonateContext;
          const left = yield* ctx.beginRun({ effect: Effect.succeed(Number(value) + 1) });
          const right = yield* ctx.beginRun({ effect: Effect.succeed(Number(value) + 2) });
          const results = yield* ctx.all([left.await, right.await]);
          return Number(results[0]) + Number(results[1]);
        }),
      DstSleep: (value) =>
        Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext> {
          const ctx = yield* ResonateContext;
          yield* ctx.sleep(Duration.seconds(Number(value)));
          return Number(value);
        }),
    }),
  );

  const program = Effect.gen(function* () {
    const client = yield* Resonate.ResonateClient;
    const counter = yield* client.beginRpc({
      targetFunction: DstCounter,
      executionId: Protocol.ExecutionId.make(`dst-${seed}-counter`),
      args: [seed],
    });
    const fanout = yield* client.beginRpc({
      targetFunction: DstFanout,
      executionId: Protocol.ExecutionId.make(`dst-${seed}-fanout`),
      args: [seed],
    });
    const sleeper = yield* client.beginRpc({
      targetFunction: DstSleep,
      executionId: Protocol.ExecutionId.make(`dst-${seed}-sleep`),
      args: [60],
    });

    for (let index = 0; index < 8; index = index + 1) {
      yield* perturb(prng);
    }
    yield* TestClock.adjust(Duration.minutes(5));
    for (let index = 0; index < 6; index = index + 1) {
      yield* perturb(prng);
    }

    expect(yield* awaitResolved(counter.id, seed)).toBe(seed + 1);
    expect(yield* awaitResolved(fanout.id, seed)).toBe(seed * 2 + 3);
    expect(yield* awaitResolved(sleeper.id, seed)).toBe(60);
    expect(yield* Ref.get(sideEffects)).toBe(1);
  });

  yield* program.pipe(
    Effect.provide(ResonateTest.layer({ group: DstFns, handlers: handlers }).pipe(Layer.provide(BunCrypto.layer))),
  );
});

const runOperationFuzzer = Effect.fn("DstSimulator.runOperationFuzzer")(function* (seed: number) {
  const prng = yield* makePrng(seed);
  const handlers = DstFns.toLayer(
    DstFns.of({
      DstCounter: Effect.succeed,
      DstFanout: Effect.succeed,
      DstSleep: Effect.succeed,
    }),
  );
  const program = Effect.gen(function* () {
    const promises = yield* DurablePromises;
    const codec = yield* currentCodec;
    const created: Array<Protocol.PromiseId> = [];

    for (let index = 0; index < 12; index = index + 1) {
      const id = Protocol.PromiseId.make(`dst-op-${seed}-${index}`);
      created.push(id);
      yield* promises.create({
        id,
        timeoutAt: DateTime.makeUnsafe(60_000 + index * 1_000),
        param: Protocol.emptyValue,
        tags: Protocol.emptyTags,
      });
      if (yield* prng.chance(0.65)) {
        yield* promises.settle({
          id,
          state: Schema.Literal("resolved").make("resolved"),
          value: yield* codec.encode({ seed, index }),
        });
      }
      yield* snapshot.pipe(Effect.flatMap(assertInvariants));
    }

    for (const id of created) {
      yield* promises.get(id).pipe(
        Effect.exit,
        Effect.flatMap((exit) => Effect.sync(() => expect(Exit.isSuccess(exit)).toBe(true))),
      );
    }
  });

  yield* program.pipe(
    Effect.provide(ResonateTest.layer({ group: DstFns, handlers: handlers }).pipe(Layer.provide(BunCrypto.layer))),
  );
});

describe("deterministic simulation", () => {
  for (const seed of [1, 7, 19]) {
    it.effect(`preserves workflow outcomes under seeded chaos ${seed}`, () => runProgramCorpus(seed));
  }

  for (const seed of [3, 11]) {
    it.effect(`preserves oracle invariants for protocol op stream ${seed}`, () => runOperationFuzzer(seed));
  }
});
