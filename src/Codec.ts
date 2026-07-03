/**
 * Payload encoding and optional encryption for durable values.
 *
 * The default codec stores native JavaScript values as Resonate protocol values
 * while preserving Error instances, infinities, undefined, and other values
 * needed by durable replay. Applications can provide {@link ResonateCodec} or
 * {@link ResonateEncryptor} to customize serialization or encryption.
 *
 * @since 0.0.0
 */
import { Context, Effect, Layer, Option, Predicate, Schema, SchemaParser, SchemaTransformation } from "effect";
import { EncodingError } from "./Errors.ts";
import type * as Protocol from "./Protocol.ts";

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

const AggregateErrorFromMarker = AggregateErrorMarker.pipe(
  Schema.decodeTo(
    AggregateErrorInstance,
    SchemaTransformation.transform({
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

const ErrorFromMarker = ErrorMarker.pipe(
  Schema.decodeTo(
    ErrorInstance,
    SchemaTransformation.transform({
      decode: (marker) => {
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

const InfinityFromSentinel = Schema.Literals([INF, NEG_INF]).pipe(
  Schema.decodeTo(
    // @effect-diagnostics-next-line schemaNumber:off
    Schema.Number.check(Schema.makeFilter((n) => n === Number.POSITIVE_INFINITY || n === Number.NEGATIVE_INFINITY)),
    SchemaTransformation.transform({
      decode: (sentinel) => (sentinel === INF ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY),
      encode: (n) => (n === Number.POSITIVE_INFINITY ? INF : NEG_INF),
    }),
  ),
);

const NativeJsonValue: Schema.Codec<unknown, unknown> = Schema.Union([
  AggregateErrorFromMarker,
  ErrorFromMarker,
  InfinityFromSentinel,
  Schema.Array(NativeJsonRef),
  Schema.Date,
  Schema.Record(Schema.String, NativeJsonRef),
  Schema.Unknown,
]);

const EmptyValueWire = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.optionalKey(Schema.Literal("")),
});

const UndefinedFromEmptyValue = EmptyValueWire.pipe(
  Schema.decodeTo(
    Schema.Undefined,
    SchemaTransformation.transform({
      decode: () => undefined,
      encode: () => EmptyValueWire.make({ headers: {}, data: "" }),
    }),
  ),
);

const ValueWithData = Schema.Struct({
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  data: Schema.NonEmptyString,
});

const DefinedValue = ValueWithData.pipe(
  Schema.decodeTo(
    Schema.StringFromBase64.pipe(Schema.decodeTo(Schema.fromJsonString(NativeJsonValue))),
    SchemaTransformation.transform({
      decode: (value) => value.data,
      encode: (data) => ValueWithData.make({ headers: {}, data }),
    }),
  ),
);

/**
 * Codec between JavaScript values and Resonate protocol values.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ValueFromUnknown = Schema.Union([UndefinedFromEmptyValue, DefinedValue]);

export interface ResonateEncryptorService {
  readonly encrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
  readonly decrypt: (value: Protocol.Value) => Effect.Effect<Protocol.Value, EncodingError>;
}

/**
 * Optional encryption service applied by the JSON codec layer.
 *
 * @category services
 * @since 0.0.0
 */
export class ResonateEncryptor extends Context.Service<ResonateEncryptor, ResonateEncryptorService>()(
  "effect-resonate/Encryptor",
) {
  static readonly serviceNoop = ResonateEncryptor.of({
    encrypt: Effect.succeed,
    decrypt: Effect.succeed,
  });

  static readonly layerNoop: Layer.Layer<ResonateEncryptor> = Layer.succeed(
    ResonateEncryptor,
    ResonateEncryptor.serviceNoop,
  );
}

/**
 * Resolves the configured encryptor or the no-op encryptor when absent.
 *
 * @category services
 * @since 0.0.0
 */
export const currentEncryptor: Effect.Effect<ResonateEncryptorService> = Effect.serviceOption(ResonateEncryptor).pipe(
  Effect.map(Option.getOrElse(() => ResonateEncryptor.serviceNoop)),
);

const hasData = SchemaParser.is(ValueWithData);

export interface ResonateCodecService {
  readonly encode: (value: unknown) => Effect.Effect<Protocol.Value, EncodingError>;
  readonly decode: (value: Protocol.Value) => Effect.Effect<unknown, EncodingError>;
}

const jsonCodec = (encryptor: Effect.Effect<ResonateEncryptorService>): ResonateCodecService => ({
  encode: Effect.fn("ResonateCodec.encode")(function* (value) {
    const current = yield* encryptor;
    const encoded = yield* Schema.encodeUnknownEffect(ValueFromUnknown)(value).pipe(
      Effect.mapError((cause) => new EncodingError({ direction: "encode", id: Option.none(), cause })),
    );
    return yield* current.encrypt(encoded);
  }),
  decode: Effect.fn("ResonateCodec.decode")(function* (value) {
    if (!hasData(value)) {
      return undefined;
    }
    const current = yield* encryptor;
    const decrypted = yield* current.decrypt(value);
    return yield* Schema.decodeUnknownEffect(ValueFromUnknown)(decrypted).pipe(
      Effect.mapError((cause) => new EncodingError({ direction: "decode", id: Option.none(), cause })),
    );
  }),
});

/**
 * Service responsible for encoding and decoding durable payloads.
 *
 * @category services
 * @since 0.0.0
 */
export class ResonateCodec extends Context.Service<ResonateCodec, ResonateCodecService>()("effect-resonate/Codec") {
  static readonly serviceJson = ResonateCodec.of(jsonCodec(currentEncryptor));

  static readonly layerJson: Layer.Layer<ResonateCodec, never, ResonateEncryptor> = Layer.effect(
    ResonateCodec,
    Effect.gen(function* () {
      return ResonateCodec.of(jsonCodec(Effect.succeed(yield* ResonateEncryptor)));
    }),
  );
}

/**
 * Resolves the configured codec or the default JSON codec when absent.
 *
 * @category services
 * @since 0.0.0
 */
export const currentCodec: Effect.Effect<ResonateCodecService> = Effect.serviceOption(ResonateCodec).pipe(
  Effect.map(Option.getOrElse(() => ResonateCodec.serviceJson)),
);

/**
 * Protocol value header used to record the logical payload schema.
 *
 * @category constants
 * @since 0.0.0
 */
export const schemaHeaderKey = "resonate:schema";

/**
 * Adds a schema name header to an encoded protocol value.
 *
 * @category combinators
 * @since 0.0.0
 */
export const withSchemaHeader = (options: {
  readonly value: Protocol.Value;
  readonly schemaName: string;
}): Protocol.Value => ({
  ...options.value,
  headers: { ...options.value.headers, [schemaHeaderKey]: options.schemaName },
});
