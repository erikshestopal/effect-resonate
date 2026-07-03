/**
 * Retry policy data model used by Resonate function invocations.
 *
 * Policies are represented as Schema classes and include codecs for the native
 * Resonate wire representation. Use the free constructors to build domain
 * values and {@link next} to compute the delay for a given attempt.
 *
 * @example
 * ```ts
 * import { Duration } from "effect"
 * import { RetryPolicy } from "effect-resonate"
 *
 * const policy = RetryPolicy.exponential({
 *   delay: Duration.seconds(1),
 *   factor: 2,
 *   maxRetries: 5,
 *   maxDelay: Duration.seconds(30)
 * })
 * ```
 *
 * @since 0.0.0
 */
import { Duration, Schema, SchemaTransformation } from "effect";

const Millis = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));
const MaxRetries = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Factor = Schema.Finite.check(Schema.isGreaterThan(0));

const ConstantFields = { delay: Millis, maxRetries: MaxRetries };
const ExponentialFields = { delay: Millis, factor: Factor, maxRetries: MaxRetries, maxDelay: Millis };
const LinearFields = { delay: Millis, maxRetries: MaxRetries };

export class Constant extends Schema.Class<Constant>("RetryPolicy/Constant")({
  _tag: Schema.tag("Constant"),
  ...ConstantFields,
}) {}

export class Exponential extends Schema.Class<Exponential>("RetryPolicy/Exponential")({
  _tag: Schema.tag("Exponential"),
  ...ExponentialFields,
}) {}

export class Linear extends Schema.Class<Linear>("RetryPolicy/Linear")({
  _tag: Schema.tag("Linear"),
  ...LinearFields,
}) {}

export class Never extends Schema.Class<Never>("RetryPolicy/Never")({
  _tag: Schema.tag("Never"),
}) {}

/**
 * Tagged union schema for retry policy domain values.
 *
 * @category schemas
 * @since 0.0.0
 */
export const RetryPolicy = Schema.Union([Constant, Exponential, Linear, Never]).pipe(Schema.toTaggedUnion("_tag"));
export type RetryPolicy = typeof RetryPolicy.Type;

/**
 * Creates a constant-delay retry policy.
 *
 * @category constructors
 * @since 0.0.0
 */
export const constant = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Constant =>
  Constant.make({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

/**
 * Creates an exponential-backoff retry policy.
 *
 * @category constructors
 * @since 0.0.0
 */
export const exponential = (options?: {
  readonly delay?: Duration.Input;
  readonly factor?: number;
  readonly maxRetries?: number;
  readonly maxDelay?: Duration.Input;
}): Exponential =>
  Exponential.make({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    factor: options?.factor ?? 2,
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
    maxDelay: Duration.toMillis(options?.maxDelay ?? Duration.seconds(30)),
  });

/**
 * Creates a linear-backoff retry policy.
 *
 * @category constructors
 * @since 0.0.0
 */
export const linear = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Linear =>
  Linear.make({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

/**
 * Creates a policy that only permits the initial attempt.
 *
 * @category constructors
 * @since 0.0.0
 */
export const never = (): Never => Never.make({});

const ConstantWire = Schema.Struct({
  type: Schema.Literal("constant"),
  data: Schema.Struct(ConstantFields),
});

const ConstantFromWire = ConstantWire.pipe(
  Schema.decodeTo(
    Constant,
    SchemaTransformation.transform({
      decode: (wire) => Constant.make(wire.data),
      encode: (policy) =>
        ConstantWire.make({ type: "constant", data: { delay: policy.delay, maxRetries: policy.maxRetries } }),
    }),
  ),
);

const ExponentialWire = Schema.Struct({
  type: Schema.Literal("exponential"),
  data: Schema.Struct(ExponentialFields),
});

const ExponentialFromWire = ExponentialWire.pipe(
  Schema.decodeTo(
    Exponential,
    SchemaTransformation.transform({
      decode: (wire) => Exponential.make(wire.data),
      encode: (policy) =>
        ExponentialWire.make({
          type: "exponential",
          data: {
            delay: policy.delay,
            factor: policy.factor,
            maxRetries: policy.maxRetries,
            maxDelay: policy.maxDelay,
          },
        }),
    }),
  ),
);

const LinearWire = Schema.Struct({
  type: Schema.Literal("linear"),
  data: Schema.Struct(LinearFields),
});

const LinearFromWire = LinearWire.pipe(
  Schema.decodeTo(
    Linear,
    SchemaTransformation.transform({
      decode: (wire) => Linear.make(wire.data),
      encode: (policy) =>
        LinearWire.make({ type: "linear", data: { delay: policy.delay, maxRetries: policy.maxRetries } }),
    }),
  ),
);

const NeverWire = Schema.Struct({
  type: Schema.Literal("never"),
  data: Schema.Struct({}),
});

const NeverFromWire = NeverWire.pipe(
  Schema.decodeTo(
    Never,
    SchemaTransformation.transform({
      decode: () => Never.make({}),
      encode: () => NeverWire.make({ type: "never", data: {} }),
    }),
  ),
);

/**
 * Codec for the Resonate wire representation of retry policies.
 *
 * @category schemas
 * @since 0.0.0
 */
export const RetryPolicyFromWire = Schema.Union([ConstantFromWire, ExponentialFromWire, LinearFromWire, NeverFromWire]);

const budget = RetryPolicy.match({
  Constant: (policy) => policy.maxRetries,
  Exponential: (policy) => policy.maxRetries,
  Linear: (policy) => policy.maxRetries,
  Never: () => 0,
});

/**
 * Computes the delay in milliseconds for an attempt, or `null` when exhausted.
 *
 * @category combinators
 * @since 0.0.0
 */
export const next = (policy: RetryPolicy, attempt: number): number | null =>
  attempt === 0
    ? 0
    : attempt > budget(policy)
      ? null
      : RetryPolicy.match(policy, {
          Constant: (policy) => policy.delay,
          Exponential: (policy) => Math.min(policy.delay * policy.factor ** attempt, policy.maxDelay),
          Linear: (policy) => policy.delay * attempt,
          Never: () => 0,
        });
