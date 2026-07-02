/**
 * Tagged error taxonomy.
 *
 * See `docs/DESIGN.md` §4.7 (Errors and cancellation) and §3.1 (the
 * platform-vs-protocol split): non-2xx protocol statuses are first-class typed
 * outcomes; only genuine transport failures become `TransportError`.
 * Mirrors the intent of `repos/resonate-sdk-ts/src/exceptions.ts`.
 */
import { Schema } from "effect";
import { PromiseId, ScheduleId, TaskId, TaskVersion } from "./Protocol.ts";

// -----------------------------------------------------------------------------
// Platform errors — never stored in promises; retriable at the transport layer
// (except Unauthorized, which is terminal — fixing the native SDK's
// retriable-auth-failure gap, DESIGN.md §6).
// -----------------------------------------------------------------------------

export class TransportError extends Schema.TaggedErrorClass<TransportError>()("TransportError", {
  reason: Schema.Literals(["ConnectionLost", "MalformedResponse", "CorrelationMismatch", "Unauthorized"]),
  cause: Schema.Defect(),
}) {}

// -----------------------------------------------------------------------------
// Protocol errors — typed outcomes of well-formed server responses
// -----------------------------------------------------------------------------

/** `409` — a mutating task op presented a stale fencing version. */
export class TaskFenced extends Schema.TaggedErrorClass<TaskFenced>()("TaskFenced", {
  id: TaskId,
  version: TaskVersion,
}) {}

/** `404` — the referenced promise does not exist. */
export class PromiseNotFound extends Schema.TaggedErrorClass<PromiseNotFound>()("PromiseNotFound", {
  id: PromiseId,
}) {}

/** `422` — the request referenced an invalid delivery target. */
export class InvalidTarget extends Schema.TaggedErrorClass<InvalidTarget>()("InvalidTarget", {
  message: Schema.String,
}) {}

/** `404` — the referenced schedule does not exist. */
export class ScheduleNotFound extends Schema.TaggedErrorClass<ScheduleNotFound>()("ScheduleNotFound", {
  id: ScheduleId,
}) {}

export type ResonateProtocolError = TaskFenced | PromiseNotFound | InvalidTarget | ScheduleNotFound;

// -----------------------------------------------------------------------------
// Terminal promise outcomes — surfaced to awaiters as typed errors
// -----------------------------------------------------------------------------

export class DurablePromiseTimedOut extends Schema.TaggedErrorClass<DurablePromiseTimedOut>()(
  "DurablePromiseTimedOut",
  { id: PromiseId },
) {}

export class DurablePromiseCanceled extends Schema.TaggedErrorClass<DurablePromiseCanceled>()(
  "DurablePromiseCanceled",
  { id: PromiseId },
) {}

// -----------------------------------------------------------------------------
// Codec boundary failures — tagged with direction and promise id, never silent
// (native: ENCODING_ARGS_* / ENCODING_RETV_* error codes)
// -----------------------------------------------------------------------------

export class EncodingError extends Schema.TaggedErrorClass<EncodingError>()("EncodingError", {
  direction: Schema.Literals(["encode", "decode"]),
  id: Schema.Option(PromiseId),
  cause: Schema.Defect(),
}) {}
