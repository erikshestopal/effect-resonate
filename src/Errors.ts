/**
 * Typed failures used by the Resonate SDK.
 *
 * Protocol clients expose expected server responses as tagged errors, while
 * transport and encoding layers keep boundary failures explicit in the Effect
 * error channel.
 *
 * @since 0.0.0
 */
import { Schema } from "effect";
import { PromiseId, ScheduleId, TaskId, TaskVersion } from "./Protocol.ts";

/**
 * Failure raised by a network transport or malformed protocol response.
 *
 * @category errors
 * @since 0.0.0
 */
export class TransportError extends Schema.TaggedErrorClass<TransportError>()("TransportError", {
  reason: Schema.Literals(["ConnectionLost", "MalformedResponse", "CorrelationMismatch", "Unauthorized"]),
  cause: Schema.Defect(),
}) {}

/**
 * Raised when a task operation targets a stale task version.
 *
 * @category errors
 * @since 0.0.0
 */
export class TaskFenced extends Schema.TaggedErrorClass<TaskFenced>()("TaskFenced", {
  id: TaskId,
  version: TaskVersion,
}) {}

/**
 * Raised when a durable promise cannot be found.
 *
 * @category errors
 * @since 0.0.0
 */
export class PromiseNotFound extends Schema.TaggedErrorClass<PromiseNotFound>()("PromiseNotFound", {
  id: PromiseId,
}) {}

/**
 * Raised when a request is invalid for the current protocol target.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidTarget extends Schema.TaggedErrorClass<InvalidTarget>()("InvalidTarget", {
  message: Schema.String,
}) {}

/**
 * Raised when a durable schedule cannot be found.
 *
 * @category errors
 * @since 0.0.0
 */
export class ScheduleNotFound extends Schema.TaggedErrorClass<ScheduleNotFound>()("ScheduleNotFound", {
  id: ScheduleId,
}) {}

export type ResonateProtocolError = TaskFenced | PromiseNotFound | InvalidTarget | ScheduleNotFound;

/**
 * Raised when awaiting a durable promise reaches its timeout.
 *
 * @category errors
 * @since 0.0.0
 */
export class DurablePromiseTimedOut extends Schema.TaggedErrorClass<DurablePromiseTimedOut>()(
  "DurablePromiseTimedOut",
  { id: PromiseId },
) {}

/**
 * Raised when awaiting a durable promise observes cancellation.
 *
 * @category errors
 * @since 0.0.0
 */
export class DurablePromiseCanceled extends Schema.TaggedErrorClass<DurablePromiseCanceled>()(
  "DurablePromiseCanceled",
  { id: PromiseId },
) {}

/**
 * Raised when durable payload encoding or decoding fails.
 *
 * @category errors
 * @since 0.0.0
 */
export class EncodingError extends Schema.TaggedErrorClass<EncodingError>()("EncodingError", {
  direction: Schema.Literals(["encode", "decode"]),
  id: Schema.Option(PromiseId),
  cause: Schema.Defect(),
}) {}
