import { describe, expect, it } from "@effect/vitest";
import { Effect } from "effect";

describe("scaffold", () => {
  it.effect("the Effect test toolchain runs", () =>
    Effect.gen(function* () {
      const value = yield* Effect.succeed(42);
      expect(value).toBe(42);
    }),
  );
});
