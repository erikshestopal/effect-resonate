import { Schema } from "effect";
import { PromiseId, ScheduleId, TaskId, TaskVersion } from "./Protocol.ts";

export class TransportError extends Schema.TaggedErrorClass<TransportError>()("TransportError", {
  reason: Schema.Literals(["ConnectionLost", "MalformedResponse", "CorrelationMismatch", "Unauthorized"]),
  cause: Schema.Defect(),
}) {}

export class TaskFenced extends Schema.TaggedErrorClass<TaskFenced>()("TaskFenced", {
  id: TaskId,
  version: TaskVersion,
}) {}

export class PromiseNotFound extends Schema.TaggedErrorClass<PromiseNotFound>()("PromiseNotFound", {
  id: PromiseId,
}) {}

export class InvalidTarget extends Schema.TaggedErrorClass<InvalidTarget>()("InvalidTarget", {
  message: Schema.String,
}) {}

export class ScheduleNotFound extends Schema.TaggedErrorClass<ScheduleNotFound>()("ScheduleNotFound", {
  id: ScheduleId,
}) {}

export type ResonateProtocolError = TaskFenced | PromiseNotFound | InvalidTarget | ScheduleNotFound;

export class DurablePromiseTimedOut extends Schema.TaggedErrorClass<DurablePromiseTimedOut>()(
  "DurablePromiseTimedOut",
  { id: PromiseId },
) {}

export class DurablePromiseCanceled extends Schema.TaggedErrorClass<DurablePromiseCanceled>()(
  "DurablePromiseCanceled",
  { id: PromiseId },
) {}

export class EncodingError extends Schema.TaggedErrorClass<EncodingError>()("EncodingError", {
  direction: Schema.Literals(["encode", "decode"]),
  id: Schema.Option(PromiseId),
  cause: Schema.Defect(),
}) {}
