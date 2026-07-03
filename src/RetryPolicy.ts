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

export const RetryPolicy = Schema.Union([Constant, Exponential, Linear, Never]).pipe(Schema.toTaggedUnion("_tag"));
export type RetryPolicy = typeof RetryPolicy.Type;

export const constant = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Constant =>
  Constant.make({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

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

export const linear = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Linear =>
  Linear.make({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

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

export const RetryPolicyFromWire = Schema.Union([ConstantFromWire, ExponentialFromWire, LinearFromWire, NeverFromWire]);

const budget = RetryPolicy.match({
  Constant: (policy) => policy.maxRetries,
  Exponential: (policy) => policy.maxRetries,
  Linear: (policy) => policy.maxRetries,
  Never: () => 0,
});

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
