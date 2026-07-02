import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { decodeValue, encodeValue, ResonateCodec, ResonateEncryptor, withSchemaHeader } from "../src/Codec.ts";
import type * as Protocol from "../src/Protocol.ts";

const layers = Layer.mergeAll(ResonateCodec.layerJson, ResonateEncryptor.layerNoop);

const fixtureError = (): Error => {
  const error = new Error("boom");
  error.name = "CardDeclined";
  error.stack = "CardDeclined: boom\n    at fixture";
  return error;
};

const fixtureAggregate = (): AggregateError => {
  const inner = new Error("inner");
  inner.stack = "Error: inner\n    at fixture";
  const aggregate = new AggregateError([inner], "many");
  aggregate.stack = "AggregateError: many\n    at fixture";
  return aggregate;
};

// Captured verbatim from `repos/resonate-sdk-ts/src/codec.ts` (see spec 02):
// each entry is the native `new Codec().encode(value)` output for the input built above.
const nativeFixtures: ReadonlyArray<{ name: string; value: () => unknown; encoded: Protocol.Value }> = [
  { name: "number", value: () => 42, encoded: { headers: {}, data: "NDI=" } },
  { name: "string", value: () => "hello", encoded: { headers: {}, data: "ImhlbGxvIg==" } },
  {
    name: "object",
    value: () => ({ a: 1, b: [true, null, "x"] }),
    encoded: { headers: {}, data: "eyJhIjoxLCJiIjpbdHJ1ZSxudWxsLCJ4Il19" },
  },
  { name: "undefined", value: () => undefined, encoded: { data: "", headers: {} } },
  { name: "+Infinity", value: () => Number.POSITIVE_INFINITY, encoded: { headers: {}, data: "Il9fSU5GX18i" } },
  { name: "-Infinity", value: () => Number.NEGATIVE_INFINITY, encoded: { headers: {}, data: "Il9fTkVHX0lORl9fIg==" } },
  {
    name: "nested infinities",
    value: () => ({ xs: [Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY] }),
    encoded: { headers: {}, data: "eyJ4cyI6WyJfX0lORl9fIiwiX19ORUdfSU5GX18iXX0=" },
  },
  {
    name: "Error",
    value: fixtureError,
    encoded: {
      headers: {},
      data: "eyJfX3R5cGUiOiJlcnJvciIsIm1lc3NhZ2UiOiJib29tIiwic3RhY2siOiJDYXJkRGVjbGluZWQ6IGJvb21cbiAgICBhdCBmaXh0dXJlIiwibmFtZSI6IkNhcmREZWNsaW5lZCJ9",
    },
  },
  {
    name: "AggregateError with nested Error",
    value: fixtureAggregate,
    encoded: {
      headers: {},
      data: "eyJfX3R5cGUiOiJhZ2dyZWdhdGVfZXJyb3IiLCJtZXNzYWdlIjoibWFueSIsInN0YWNrIjoiQWdncmVnYXRlRXJyb3I6IG1hbnlcbiAgICBhdCBmaXh0dXJlIiwibmFtZSI6IkFnZ3JlZ2F0ZUVycm9yIiwiZXJyb3JzIjpbeyJfX3R5cGUiOiJlcnJvciIsIm1lc3NhZ2UiOiJpbm5lciIsInN0YWNrIjoiRXJyb3I6IGlubmVyXG4gICAgYXQgZml4dHVyZSIsIm5hbWUiOiJFcnJvciJ9XX0=",
    },
  },
  { name: "unicode", value: () => "héllo 🌍 ∞", encoded: { headers: {}, data: "ImjDqWxsbyDwn4yNIOKIniI=" } },
];

describe("byte compatibility with the native TS codec", () => {
  it.effect.each(nativeFixtures)("encodes $name exactly as native", ({ encoded, value }) =>
    Effect.gen(function* () {
      expect(yield* encodeValue(value())).toEqual(encoded);
    }).pipe(Effect.provide(layers)),
  );

  it.effect("decodes a native-encoded payload (reverse direction)", () =>
    Effect.gen(function* () {
      const object = nativeFixtures[2];
      const infinities = nativeFixtures[6];
      if (!object || !infinities) {
        throw new Error("fixture missing");
      }
      expect(yield* decodeValue(object.encoded)).toEqual({ a: 1, b: [true, null, "x"] });
      expect(yield* decodeValue(infinities.encoded)).toEqual({
        xs: [Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY],
      });
      expect(yield* decodeValue({ data: "", headers: {} })).toBeUndefined();
      expect(yield* decodeValue({})).toBeUndefined();
    }).pipe(Effect.provide(layers)),
  );
});

describe("rejection round-trips", () => {
  it.effect("an encoded Error decodes to an Error with message/name/stack preserved", () =>
    Effect.gen(function* () {
      const decoded = yield* encodeValue(fixtureError()).pipe(Effect.flatMap(decodeValue));
      expect(decoded).toBeInstanceOf(Error);
      if (!(decoded instanceof Error)) {
        return;
      }
      expect(decoded.message).toBe("boom");
      expect(decoded.name).toBe("CardDeclined");
      expect(decoded.stack).toBe("CardDeclined: boom\n    at fixture");
    }).pipe(Effect.provide(layers)),
  );

  it.effect("an encoded AggregateError decodes with reconstructed nested errors", () =>
    Effect.gen(function* () {
      const decoded = yield* encodeValue(fixtureAggregate()).pipe(Effect.flatMap(decodeValue));
      expect(decoded).toBeInstanceOf(AggregateError);
      if (!(decoded instanceof AggregateError)) {
        return;
      }
      expect(decoded.message).toBe("many");
      expect(decoded.errors).toHaveLength(1);
      expect(decoded.errors[0]).toBeInstanceOf(Error);
      expect(decoded.errors[0].message).toBe("inner");
    }).pipe(Effect.provide(layers)),
  );
});

describe("round-trip property (JSON-representable values)", () => {
  const samples: ReadonlyArray<unknown> = [
    0,
    -1.5,
    1e21,
    "",
    'with "quotes" and \\ backslashes',
    true,
    false,
    null,
    [],
    {},
    [1, "two", null, { three: 3 }],
    { deeply: { nested: { structure: [{ a: [1, 2, 3] }] } } },
    { unicode: "❄️ ünïcode", empty: "", zero: 0 },
  ];

  it.effect.each(samples.map((value, index) => ({ index, value })))("decode(encode(v)) = v [#$index]", ({ value }) =>
    Effect.gen(function* () {
      expect(yield* encodeValue(value).pipe(Effect.flatMap(decodeValue))).toEqual(value);
    }).pipe(Effect.provide(layers)),
  );
});

describe("encryptor seam", () => {
  // A toy byte-level transform: prefixes the payload, proving both directions run.
  const layerReversing = Layer.succeed(
    ResonateEncryptor,
    ResonateEncryptor.of({
      encrypt: (value) => Effect.succeed({ ...value, data: `enc:${value.data ?? ""}` }),
      decrypt: (value) => Effect.succeed({ ...value, data: (value.data ?? "").replace(/^enc:/, "") }),
    }),
  );

  it.effect("a custom encryptor applies after encode and before decode", () =>
    Effect.gen(function* () {
      const encoded = yield* encodeValue(42);
      expect(encoded.data).toBe("enc:NDI=");
      expect(yield* decodeValue(encoded)).toBe(42);
    }).pipe(Effect.provide(Layer.mergeAll(ResonateCodec.layerJson, layerReversing))),
  );

  it.effect("empty data short-circuits decode before the encryptor runs (native order)", () =>
    Effect.gen(function* () {
      const poisoned = Layer.succeed(
        ResonateEncryptor,
        ResonateEncryptor.of({
          encrypt: Effect.succeed,
          decrypt: () => Effect.die(new Error("decrypt must not run for empty data")),
        }),
      );
      const result = yield* decodeValue({ data: "", headers: {} }).pipe(
        Effect.provide(Layer.mergeAll(ResonateCodec.layerJson, poisoned)),
      );
      expect(result).toBeUndefined();
    }),
  );
});

describe("schema header", () => {
  it("annotates values additively", () => {
    expect(withSchemaHeader({ data: "NDI=", headers: { a: "b" } }, "Countdown/payload")).toEqual({
      data: "NDI=",
      headers: { a: "b", "resonate:schema": "Countdown/payload" },
    });
  });
});
