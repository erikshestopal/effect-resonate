/**
 * Value encoding boundary.
 *
 * See `docs/DESIGN.md` §4.10 (Codecs). Every value that crosses the wire —
 * function args, return values, rejection values, external-promise payloads —
 * passes through this one boundary producing the protocol's `Value` shape.
 *
 * The default codec is byte-compatible with the native TS SDK
 * (`repos/resonate-sdk-ts/src/codec.ts`), expressed as one Schema codec:
 * a recursive union modeling the native JSON tree (`__type` markers
 * reconstructing `Error`/`AggregateError`, `"__INF__"`/`"__NEG_INF__"`
 * Infinity sentinels), piped through JSON and base64; `undefined` ⇄ empty data.
 */
import { Context, Effect, Layer, Option, Predicate, Schema, SchemaParser, SchemaTransformation } from "effect";
import { EncodingError } from "./Errors.ts";
import type * as Protocol from "./Protocol.ts";

// -----------------------------------------------------------------------------
// The native JSON tree — a recursive union over every substitution the native
// replacer/reviver performs, anywhere in the value
// -----------------------------------------------------------------------------

const INF = "__INF__";
const NEG_INF = "__NEG_INF__";

const NativeJsonRef = Schema.suspend((): Schema.Codec<unknown, unknown> => NativeJsonValue);

const AggregateErrorMarker = Schema.Struct({
  __type: Schema.tag("aggregate_error"),
  message: Schema.optionalKey(Schema.Unknown),
  stack: Schema.optionalKey(Schema.Unknown),
  name: Schema.optionalKey(Schema.Unknown),
  errors: Schema.Array(NativeJsonRef),
});

const AggregateErrorInstance = Schema.instanceOf(AggregateError);

/** `{__type:"aggregate_error", ...}` ⇄ `AggregateError` (nested errors recurse). */
const AggregateErrorFromMarker = AggregateErrorMarker.pipe(
  Schema.decodeTo(
    AggregateErrorInstance,
    SchemaTransformation.transform({
      // Native: Object.assign(new AggregateError(v.errors, v.message), v) —
      // marker fields (including __type) are copied verbatim onto the instance,
      // so the assign overwrites whatever message the constructor coerced.
      decode: (marker) =>
        AggregateErrorInstance.make(
          Object.assign(
            new AggregateError(marker.errors, Predicate.isString(marker.message) ? marker.message : undefined),
            marker,
          ),
        ),
      encode: (error) =>
        AggregateErrorMarker.make({
          message: error.message,
          stack: error.stack,
          name: error.name,
          errors: error.errors,
        }),
    }),
  ),
);

const ErrorMarker = Schema.Struct({
  __type: Schema.tag("error"),
  message: Schema.optionalKey(Schema.Unknown),
  stack: Schema.optionalKey(Schema.Unknown),
  name: Schema.optionalKey(Schema.Unknown),
});

const ErrorInstance = Schema.instanceOf(Error);

/** `{__type:"error", ...}` ⇄ `Error`, with the native truthiness coercions. */
const ErrorFromMarker = ErrorMarker.pipe(
  Schema.decodeTo(
    ErrorInstance,
    SchemaTransformation.transform({
      // Native: new Error(v.message || "Unknown error"); name/stack assigned only when truthy.
      decode: (marker) => {
        // Reconstructing the native SDK's serialized error, not raising one.
        // ast-grep-ignore: no-new-error
        const error = new Error(Predicate.isTruthy(marker.message) ? String(marker.message) : "Unknown error");
        return ErrorInstance.make(
          Object.assign(
            error,
            Predicate.isTruthy(marker.name) ? { name: marker.name } : {},
            Predicate.isTruthy(marker.stack) ? { stack: marker.stack } : {},
          ),
        );
      },
      encode: (error) =>
        ErrorMarker.make({
          message: error.message,
          stack: error.stack,
          name: error.name,
        }),
    }),
  ),
);

/** `"__INF__"`/`"__NEG_INF__"` ⇄ `±Infinity`. */
const InfinityFromSentinel = Schema.Literals([INF, NEG_INF]).pipe(
  Schema.decodeTo(
    Schema.Number.check(
      Schema.makeFilter((n: number) => n === Number.POSITIVE_INFINITY || n === Number.NEGATIVE_INFINITY, {
        title: "an infinite number",
      }),
    ),
    SchemaTransformation.transform({
      decode: (sentinel) => (sentinel === INF ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY),
      encode: (n) => (n === Number.POSITIVE_INFINITY ? INF : NEG_INF),
    }),
  ),
);

/**
 * Member order is the dispatch order on both sides: AggregateError before Error
 * (subtype), markers/sentinels before the structural members, `Schema.Date`
 * before `Record` so a `Date` leaf passes through untouched and `JSON.stringify`
 * applies its `toJSON` (as the native replacer does), `Unknown` as the
 * passthrough for every other leaf.
 */
const NativeJsonValue: Schema.Codec<unknown, unknown> = Schema.Union([
  AggregateErrorFromMarker,
  ErrorFromMarker,
  InfinityFromSentinel,
  Schema.Array(NativeJsonRef),
  Schema.Date,
  Schema.Record(Schema.String, NativeJsonRef),
  Schema.Unknown,
]);

// -----------------------------------------------------------------------------
// The full Value codec: unknown ⇄ { headers, data: base64(json(tree)) }
// -----------------------------------------------------------------------------

const EmptyValueWire = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.optionalKey(Schema.Literal("")),
});

/** `undefined` ⇄ `{headers:{}, data:""}`; decode accepts any record with empty/missing data. */
const UndefinedFromEmptyValue = EmptyValueWire.pipe(
  Schema.decodeTo(
    Schema.Undefined,
    SchemaTransformation.transform({
      decode: () => undefined,
      encode: () => EmptyValueWire.make({ headers: {}, data: "" }),
    }),
  ),
);

/** A `Value` actually carrying data (native `value?.data` truthiness). */
const ValueWithData = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.NonEmptyString,
});

const DefinedValue = ValueWithData.pipe(
  Schema.decodeTo(
    Schema.StringFromBase64.pipe(Schema.decodeTo(Schema.fromJsonString(NativeJsonValue))),
    SchemaTransformation.transform({
      // Native decode reads only `data`; native encode always emits `headers: {}`.
      decode: (value) => value.data,
      encode: (data) => ValueWithData.make({ headers: {}, data }),
    }),
  ),
);

/** The native codec as one Schema: decoded side `unknown`, encoded side `Protocol.Value`. */
export const ValueFromUnknown = Schema.Union([UndefinedFromEmptyValue, DefinedValue]);

// -----------------------------------------------------------------------------
// Services
// -----------------------------------------------------------------------------

/** Byte-level transform applied after encode / before decode (crypto, compression). */
export class ResonateEncryptor extends Context.Service<
  ResonateEncryptor,
  {
    readonly encrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
    readonly decrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
  }
>()("effect-resonate/Encryptor") {
  /** Default: pass-through, matching the native `NoopEncryptor`. */
  static readonly layerNoop: Layer.Layer<ResonateEncryptor> = Layer.succeed(
    ResonateEncryptor,
    ResonateEncryptor.of({
      encrypt: Effect.succeed,
      decrypt: Effect.succeed,
    }),
  );
}

const hasData = SchemaParser.is(ValueWithData);

/**
 * Value-level encoding. The implementation composes the `ResonateEncryptor`
 * around the JSON codec, exactly like the native `Codec` class: encrypt runs
 * after encode, decrypt before decode, and missing data short-circuits decode
 * to `undefined` BEFORE decrypting.
 */
export class ResonateCodec extends Context.Service<
  ResonateCodec,
  {
    readonly encode: (value: unknown) => Effect.Effect<Protocol.Value, EncodingError>;
    readonly decode: (value: Protocol.Value) => Effect.Effect<unknown, EncodingError>;
  }
>()("effect-resonate/Codec") {
  /** Default: byte-compatible with the native TS SDK; encryptor from context. */
  static readonly layerJson: Layer.Layer<ResonateCodec, never, ResonateEncryptor> = Layer.effect(
    ResonateCodec,
    Effect.gen(function* () {
      const encryptor = yield* ResonateEncryptor;

      const encode = Effect.fn("ResonateCodec.encode")(function* (value: unknown) {
        const encoded = yield* Schema.encodeUnknownEffect(ValueFromUnknown)(value).pipe(
          Effect.mapError((cause) => new EncodingError({ direction: "encode", id: Option.none(), cause })),
        );
        return yield* encryptor.encrypt(encoded);
      });

      const decode = Effect.fn("ResonateCodec.decode")(function* (value: Protocol.Value) {
        // Native checks for missing data BEFORE decrypting.
        if (!hasData(value)) {
          return undefined;
        }
        const decrypted = yield* encryptor.decrypt(value);
        return yield* Schema.decodeUnknownEffect(ValueFromUnknown)(decrypted).pipe(
          Effect.mapError((cause) => new EncodingError({ direction: "decode", id: Option.none(), cause })),
        );
      });

      return ResonateCodec.of({ encode, decode });
    }),
  );
}

// -----------------------------------------------------------------------------
// Schema-version header (additive; other SDKs ignore it — DESIGN §4.10)
// -----------------------------------------------------------------------------

export const schemaHeaderKey = "resonate:schema";

/** Annotate an encoded value with the name of the payload schema, where known. */
export const withSchemaHeader = (value: Protocol.Value, schemaName: string): Protocol.Value => ({
  ...value,
  headers: { ...value.headers, [schemaHeaderKey]: schemaName },
});
