/**
 * High-level durable schedule authoring API.
 *
 * @since 0.0.0
 */
import { Array as Arr, Cron, Duration, Effect, Layer, Number as Num, Order, Predicate, Schema } from "effect";
import { currentCodec, withSchemaHeader } from "./Codec.ts";
import type { AnyFunction, PayloadArgs } from "./FunctionDefinition.ts";
import { InvocationParam } from "./Invocation.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import { Schedules } from "./Schedule.ts";

const globalScope = Schema.Literal("global").make("global");

/**
 * Options for defining a durable schedule.
 *
 * @category models
 * @since 0.0.0
 */
export interface ScheduleOptions<F extends AnyFunction> {
  readonly id: Protocol.ScheduleId;
  readonly cron: Cron.Cron;
  readonly function: F;
  readonly payload: PayloadArgs<F>;
  readonly timeout?: Duration.Duration;
  readonly target?: Protocol.WorkerGroup;
  readonly tags?: Protocol.Tags;
  readonly version?: Protocol.FunctionVersionOrLatest;
  readonly retryPolicy?: RetryPolicy.RetryPolicy;
}

/**
 * Durable schedule definition with effects for creating, reading, and deleting it.
 *
 * @category models
 * @since 0.0.0
 */
export interface ScheduleValue<F extends AnyFunction = AnyFunction> {
  readonly id: Protocol.ScheduleId;
  readonly cron: Cron.Cron;
  readonly definition: F;
  readonly payload: PayloadArgs<F>;
  readonly create: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules | ResonateNetwork>;
  readonly get: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules>;
  readonly delete: Effect.Effect<void, unknown, Schedules>;
  readonly layer: Layer.Layer<never, unknown, Schedules | ResonateNetwork>;
}

interface CronSegmentOptions {
  readonly values: ReadonlySet<number>;
  readonly min: number;
  readonly max: number;
}

const fullCronSegment = (options: CronSegmentOptions): boolean => {
  if (options.values.size === 0) {
    return true;
  }
  if (options.values.size !== Num.increment(options.max - options.min)) {
    return false;
  }
  return Arr.every(Arr.range(options.min, options.max), (value) => options.values.has(value));
};

const cronSegment = (options: CronSegmentOptions): string => {
  if (fullCronSegment(options)) {
    return "*";
  }
  return Arr.sort(options.values, Order.Number).join(",");
};

const fiveFieldCronExpression = (cron: Cron.Cron): Effect.Effect<string, never> => {
  if (cron.seconds.size !== 1 || !cron.seconds.has(0)) {
    return Effect.die("Resonate schedules only support five-field cron expressions");
  }
  return Effect.succeed(
    [
      cronSegment({ values: cron.minutes, min: 0, max: 59 }),
      cronSegment({ values: cron.hours, min: 0, max: 23 }),
      cronSegment({ values: cron.days, min: 1, max: 31 }),
      cronSegment({ values: cron.months, min: 1, max: 12 }),
      cronSegment({ values: cron.weekdays, min: 0, max: 6 }),
    ].join(" "),
  );
};

/**
 * Defines a durable schedule for invoking a function on a cron expression.
 *
 * @category constructors
 * @since 0.0.0
 */
export const schedule = <F extends AnyFunction>(options: ScheduleOptions<F>): ScheduleValue<F> => {
  const timeout = options.timeout ?? Duration.hours(24);
  const tags = options.tags ?? Protocol.emptyTags;
  const version = options.version ?? options.function.version;
  const retry = options.retryPolicy;

  const create: ScheduleValue<F>["create"] = Effect.gen(function* () {
    const schedules = yield* Schedules;
    const codec = yield* currentCodec;
    const network = yield* ResonateNetwork;
    const encodedArgs = yield* Schema.encodeUnknownEffect(options.function.payload)(options.payload).pipe(
      Effect.catchCause(() =>
        options.payload.length === 1
          ? Schema.encodeUnknownEffect(options.function.payload)(options.payload[0])
          : Effect.die("Invalid function payload"),
      ),
    );
    const encoded = yield* codec.encode(
      InvocationParam.make({
        func: options.function.name,
        args: Arr.ensure(encodedArgs),
        version,
        ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
      }),
    );
    const target = network.match(options.target ?? Protocol.WorkerGroup.make("default"));
    return yield* schedules.create({
      id: options.id,
      cron: yield* fiveFieldCronExpression(options.cron),
      promiseId: "{{.id}}.{{.timestamp}}",
      promiseTimeout: timeout,
      promiseParam: withSchemaHeader({ value: encoded, schemaName: options.function.name }),
      promiseTags: Protocol.Tags.make({
        reserved: {
          ...tags.reserved,
          "resonate:target": target,
          "resonate:scope": globalScope,
        },
        unrecognized: tags.unrecognized,
        user: tags.user,
      }),
    });
  });

  return {
    id: options.id,
    cron: options.cron,
    definition: options.function,
    payload: options.payload,
    create,
    get: Schedules.pipe(Effect.flatMap((schedules) => schedules.get(options.id))),
    delete: Schedules.pipe(Effect.flatMap((schedules) => schedules.delete(options.id))),
    layer: Layer.effectDiscard(create),
  };
};
