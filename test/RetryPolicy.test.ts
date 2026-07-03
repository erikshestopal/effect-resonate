import { describe, expect, it } from "@effect/vitest";
import { Duration, Schema } from "effect";
import * as RetryPolicy from "../src/RetryPolicy.ts";

describe("RetryPolicy", () => {
  it("matches native delay math", () => {
    expect([0, 1, 2, 3].map((attempt) => RetryPolicy.next(RetryPolicy.constant(), attempt))).toEqual([
      0, 1_000, 1_000, 1_000,
    ]);
    expect([0, 1, 2, 3, 4, 5].map((attempt) => RetryPolicy.next(RetryPolicy.exponential(), attempt))).toEqual([
      0, 2_000, 4_000, 8_000, 16_000, 30_000,
    ]);
    expect([0, 1, 2, 3].map((attempt) => RetryPolicy.next(RetryPolicy.linear(), attempt))).toEqual([
      0, 1_000, 2_000, 3_000,
    ]);
    expect([0, 1, 2].map((attempt) => RetryPolicy.next(RetryPolicy.never(), attempt))).toEqual([0, null, null]);
  });

  it("encodes wire fixtures like the native SDK", () => {
    expect(Schema.encodeUnknownSync(RetryPolicy.RetryPolicyFromWire)(RetryPolicy.constant())).toEqual({
      type: "constant",
      data: { delay: 1_000, maxRetries: Number.MAX_SAFE_INTEGER },
    });
    expect(
      Schema.encodeUnknownSync(RetryPolicy.RetryPolicyFromWire)(
        RetryPolicy.exponential({
          delay: Duration.seconds(2),
          factor: 3,
          maxRetries: 4,
          maxDelay: Duration.seconds(20),
        }),
      ),
    ).toEqual({
      type: "exponential",
      data: { delay: 2_000, factor: 3, maxRetries: 4, maxDelay: 20_000 },
    });
    expect(Schema.encodeUnknownSync(RetryPolicy.RetryPolicyFromWire)(RetryPolicy.linear())).toEqual({
      type: "linear",
      data: { delay: 1_000, maxRetries: Number.MAX_SAFE_INTEGER },
    });
    expect(Schema.encodeUnknownSync(RetryPolicy.RetryPolicyFromWire)(RetryPolicy.never())).toEqual({
      type: "never",
      data: {},
    });
  });
});
