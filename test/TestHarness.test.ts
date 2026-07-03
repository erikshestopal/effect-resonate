import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Duration, Effect, Exit, Layer, Option, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { DurablePromises } from "../src/DurablePromise.ts";
import * as Protocol from "../src/Protocol.ts";
import * as Resonate from "../src/Resonate.ts";
import { ResonateContext } from "../src/ResonateContext.ts";
import { assertInvariants, ResonateTest, restartWorker, snapshot } from "../src/testing.ts";

const Countdown = Resonate.function({
  name: "HarnessCountdown",
  payload: Schema.Tuple([Schema.Number, Schema.Number]),
});

const Replay = Resonate.function({ name: "HarnessReplay", payload: Schema.Number });

const AppFns = Resonate.group(Countdown, Replay);

describe("ResonateTest", () => {
  it.effect("runs the DESIGN countdown example against the public test layer", () => {
    const HandlersLive = AppFns.toLayer(
      AppFns.of({
        HarnessCountdown: (count, seconds) =>
          Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
            const ctx = yield* ResonateContext;
            for (let remaining = count; remaining > 0; remaining = remaining - 1) {
              yield* ctx.sleep(Duration.seconds(seconds));
            }
            return "done";
          }),
        HarnessReplay: () => Effect.void,
      }),
    );

    return Effect.gen(function* () {
      const client = yield* Resonate.ResonateClient;
      const handle = yield* client.beginRpc({
        targetFunction: Countdown,
        executionId: Protocol.ExecutionId.make("harness-countdown-1"),
        args: [3, 60],
      });

      yield* TestClock.adjust(Duration.minutes(3));
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      const exit = yield* handle.poll;
      expect(Option.isSome(exit)).toBe(true);
      if (Option.isSome(exit)) {
        expect(Exit.isSuccess(exit.value)).toBe(true);
      }
      yield* snapshot.pipe(Effect.flatMap(assertInvariants));
    }).pipe(
      Effect.provide(
        ResonateTest.layer({ group: AppFns, handlers: HandlersLive }).pipe(Layer.provide(BunCrypto.layer)),
      ),
    );
  });

  it.effect("restarts the worker and replays recorded local steps exactly once", () =>
    Effect.gen(function* () {
      const stepCalls = yield* Ref.make(0);
      const HandlersLive = AppFns.toLayer(
        AppFns.of({
          HarnessCountdown: () => Effect.void,
          HarnessReplay: (seconds) =>
            Effect.gen(function* (): Effect.fn.Return<string, unknown, ResonateContext> {
              const ctx = yield* ResonateContext;
              yield* ctx.run({ effect: Ref.update(stepCalls, (calls) => calls + 1) });
              yield* ctx.sleep(Duration.seconds(seconds));
              return "replayed";
            }),
        }),
      );

      const program = Effect.gen(function* () {
        const client = yield* Resonate.ResonateClient;
        const promises = yield* DurablePromises;
        const handle = yield* client.beginRpc({
          targetFunction: Replay,
          executionId: Protocol.ExecutionId.make("harness-replay-1"),
          args: [60],
        });
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;

        const suspended = yield* snapshot;
        expect(suspended.tasks.find((task) => task.id === handle.id)?.state).toBe("suspended");
        expect(yield* Ref.get(stepCalls)).toBe(1);

        yield* restartWorker;
        yield* TestClock.adjust(Duration.minutes(1));
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;

        const completed = yield* promises.get(handle.id);
        expect(completed.state).toBe("resolved");
        expect(yield* Ref.get(stepCalls)).toBe(1);
        yield* snapshot.pipe(Effect.flatMap(assertInvariants));
      });

      yield* program.pipe(
        Effect.provide(
          ResonateTest.layer({ group: AppFns, handlers: HandlersLive }).pipe(Layer.provide(BunCrypto.layer)),
        ),
      );
    }),
  );
});
