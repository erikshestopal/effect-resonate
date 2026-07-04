/**
 * Effect-native SDK for the Resonate durable execution protocol.
 *
 * The package is organized as small public modules. Use {@link Resonate} for
 * the high-level client and function definition API, {@link Worker} to run
 * registered handlers, and {@link Protocol} when integrating with the Resonate
 * wire protocol directly.
 *
 * @since 0.0.0
 */

/**
 * Encoding and encryption services for durable payloads.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Codec from "./Codec.ts";

/**
 * Low-level durable promise protocol client service.
 *
 * @category modules
 * @since 0.0.0
 */
export * as DurablePromise from "./DurablePromise.ts";

/**
 * Typed errors raised by protocol clients, codecs, and handles.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Errors from "./Errors.ts";

/**
 * Durable function definitions and typed handler groups.
 *
 * @category modules
 * @since 0.0.0
 */
export * as FunctionDefinition from "./FunctionDefinition.ts";

/**
 * Shared invocation payload schemas.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Invocation from "./Invocation.ts";

/**
 * Runtime-neutral Resonate network service interface.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Network from "./network/network.ts";

/**
 * HTTP implementation of the Resonate network service.
 *
 * @category modules
 * @since 0.0.0
 */
export * as NetworkHttp from "./network/http.ts";

/**
 * In-memory Resonate network implementation for local execution.
 *
 * @category modules
 * @since 0.0.0
 */
export * as NetworkLocal from "./network/local.ts";

/**
 * Schema-first Resonate protocol model and wire codecs.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Protocol from "./Protocol.ts";

/**
 * External durable promise declarations.
 *
 * @category modules
 * @since 0.0.0
 */
export * as PromiseDefinition from "./PromiseDefinition.ts";

/**
 * Durable function handler registry model.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Registry from "./Registry.ts";

/**
 * High-level function, schedule, promise, and client APIs.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Resonate from "./Resonate.ts";

/**
 * High-level durable schedule authoring API.
 *
 * @category modules
 * @since 0.0.0
 */
export * as ScheduleDefinition from "./ScheduleDefinition.ts";

/**
 * Durable execution context available inside registered handlers.
 *
 * @category modules
 * @since 0.0.0
 */
export * as ResonateContext from "./ResonateContext.ts";

/**
 * Retry policy constructors and wire codecs.
 *
 * @category modules
 * @since 0.0.0
 */
export * as RetryPolicy from "./RetryPolicy.ts";

/**
 * Low-level durable schedule protocol client service.
 *
 * @category modules
 * @since 0.0.0
 */
export * as ResonateSchedule from "./Schedule.ts";

/**
 * Low-level durable task protocol client service.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Task from "./Task.ts";

/**
 * Worker layers for executing registered function handlers.
 *
 * @category modules
 * @since 0.0.0
 */
export * as Worker from "./Worker.ts";
