import { Duration, Match, Schema, SchemaTransformation } from "effect";

const Millis = Schema.Finite;
const MaxRetries = Schema.Finite;

export class Constant extends Schema.Class<Constant>("RetryPolicy/Constant")({
  _tag: Schema.tag("Constant"),
  delay: Millis,
  maxRetries: MaxRetries,
}) {
  static default(): Constant {
    return new Constant({ delay: 1_000, maxRetries: Number.MAX_SAFE_INTEGER });
  }
}

export class Exponential extends Schema.Class<Exponential>("RetryPolicy/Exponential")({
  _tag: Schema.tag("Exponential"),
  delay: Millis,
  factor: Schema.Finite,
  maxRetries: MaxRetries,
  maxDelay: Millis,
}) {
  static default(): Exponential {
    return new Exponential({ delay: 1_000, factor: 2, maxRetries: Number.MAX_SAFE_INTEGER, maxDelay: 30_000 });
  }
}

export class Linear extends Schema.Class<Linear>("RetryPolicy/Linear")({
  _tag: Schema.tag("Linear"),
  delay: Millis,
  maxRetries: MaxRetries,
}) {
  static default(): Linear {
    return new Linear({ delay: 1_000, maxRetries: Number.MAX_SAFE_INTEGER });
  }
}

export class Never extends Schema.Class<Never>("RetryPolicy/Never")({
  _tag: Schema.tag("Never"),
}) {
  static default(): Never {
    return new Never();
  }
}

export const RetryPolicy = Schema.Union([Constant, Exponential, Linear, Never]);
export type RetryPolicy = typeof RetryPolicy.Type;

export const constant = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Constant =>
  new Constant({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

export const exponential = (options?: {
  readonly delay?: Duration.Input;
  readonly factor?: number;
  readonly maxRetries?: number;
  readonly maxDelay?: Duration.Input;
}): Exponential =>
  new Exponential({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    factor: options?.factor ?? 2,
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
    maxDelay: Duration.toMillis(options?.maxDelay ?? Duration.seconds(30)),
  });

export const linear = (options?: { readonly delay?: Duration.Input; readonly maxRetries?: number }): Linear =>
  new Linear({
    delay: Duration.toMillis(options?.delay ?? Duration.seconds(1)),
    maxRetries: options?.maxRetries ?? Number.MAX_SAFE_INTEGER,
  });

export const never = (): Never => new Never();

class WireConstant extends Schema.Class<WireConstant>("RetryPolicy/WireConstant")({
  type: Schema.Literal("constant"),
  data: Schema.Struct({ delay: Millis, maxRetries: MaxRetries }),
}) {}

class WireExponential extends Schema.Class<WireExponential>("RetryPolicy/WireExponential")({
  type: Schema.Literal("exponential"),
  data: Schema.Struct({ delay: Millis, factor: Schema.Finite, maxRetries: MaxRetries, maxDelay: Millis }),
}) {}

class WireLinear extends Schema.Class<WireLinear>("RetryPolicy/WireLinear")({
  type: Schema.Literal("linear"),
  data: Schema.Struct({ delay: Millis, maxRetries: MaxRetries }),
}) {}

class WireNever extends Schema.Class<WireNever>("RetryPolicy/WireNever")({
  type: Schema.Literal("never"),
  data: Schema.Struct({}),
}) {}

export const RetryPolicyFromWire = Schema.Union([WireConstant, WireExponential, WireLinear, WireNever]).pipe(
  Schema.decodeTo(
    RetryPolicy,
    SchemaTransformation.transform({
      decode: (wire) =>
        Match.value(wire).pipe(
          Match.discriminatorsExhaustive("type")({
            constant: (policy) => new Constant(policy.data),
            exponential: (policy) => new Exponential(policy.data),
            linear: (policy) => new Linear(policy.data),
            never: () => new Never(),
          }),
        ),
      encode: (policy) =>
        Match.value(policy).pipe(
          Match.discriminatorsExhaustive("_tag")({
            Constant: (policy) =>
              WireConstant.make({ type: "constant", data: { delay: policy.delay, maxRetries: policy.maxRetries } }),
            Exponential: (policy) =>
              WireExponential.make({
                type: "exponential",
                data: {
                  delay: policy.delay,
                  factor: policy.factor,
                  maxRetries: policy.maxRetries,
                  maxDelay: policy.maxDelay,
                },
              }),
            Linear: (policy) =>
              WireLinear.make({ type: "linear", data: { delay: policy.delay, maxRetries: policy.maxRetries } }),
            Never: () => WireNever.make({ type: "never", data: {} }),
          }),
        ),
    }),
  ),
);

export const next = (policy: RetryPolicy, attempt: number): number | null =>
  Match.value(policy).pipe(
    Match.discriminatorsExhaustive("_tag")({
      Constant: (policy) => (attempt > policy.maxRetries ? null : attempt === 0 ? 0 : policy.delay),
      Exponential: (policy) =>
        attempt > policy.maxRetries
          ? null
          : attempt === 0
            ? 0
            : Math.min(policy.delay * policy.factor ** attempt, policy.maxDelay),
      Linear: (policy) => (attempt > policy.maxRetries ? null : policy.delay * attempt),
      Never: () => (attempt === 0 ? 0 : null),
    }),
  );
