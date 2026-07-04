/**
 * High-level durable schedule authoring API.
 *
 * @since 0.0.0
 */
import { Array as Arr, Duration, Effect, Layer, Predicate, Schema } from "effect";
import { currentCodec } from "./Codec.ts";
import { InvocationParam, type AnyFunction, type PayloadArgs } from "./FunctionDefinition.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";
import * as RetryPolicy from "./RetryPolicy.ts";
import { Schedules } from "./Schedule.ts";

export type CronExpression = string;

/**
 * Options for defining a durable schedule.
 *
 * @category models
 * @since 0.0.0
 */
export interface ScheduleOptions<F extends AnyFunction> {
  readonly id: Protocol.ScheduleId;
  readonly cron: CronExpression;
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
  readonly cron: CronExpression;
  readonly definition: F;
  readonly payload: PayloadArgs<F>;
  readonly create: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules | ResonateNetwork>;
  readonly get: Effect.Effect<Protocol.ScheduleRecord, unknown, Schedules>;
  readonly delete: Effect.Effect<void, unknown, Schedules>;
  readonly layer: Layer.Layer<never, unknown, Schedules | ResonateNetwork>;
}

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
    const invocation = InvocationParam.make({
      func: options.function.name,
      args: Arr.ensure(encodedArgs),
      version,
      ...(Predicate.isNotUndefined(retry) ? { retry } : {}),
    });
    const encodedInvocation = yield* Schema.encodeUnknownEffect(InvocationParam)(invocation);
    const encoded = yield* codec.encode(encodedInvocation);
    const target = network.match(options.target ?? Protocol.WorkerGroup.make("default"));
    return yield* schedules.create(
      Protocol.ScheduleCreateData.make({
        id: options.id,
        cron: options.cron,
        promiseId: "{{.id}}.{{.timestamp}}",
        promiseTimeout: timeout,
        promiseParam: encoded,
        promiseTags: Protocol.Tags.make({
          reserved: {
            ...tags.reserved,
            "resonate:target": target,
          },
          unrecognized: tags.unrecognized,
          user: tags.user,
        }),
      }),
    );
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
