import { DateTime, Duration, Effect, Order, Schema } from "effect";
export const Priority = Schema.Literals(["critical", "high", "normal", "low"]);
export type Priority = typeof Priority.Type;
export const Job = Schema.Struct({
  id: Schema.String,
  priority: Priority,
  description: Schema.String,
  workMs: Schema.Finite,
});
export type Job = typeof Job.Type;
export const JobResult = Schema.Struct({
  id: Schema.String,
  priority: Priority,
  description: Schema.String,
  startedAt: Schema.String,
  completedAt: Schema.String,
  durationMs: Schema.Finite,
  queuePosition: Schema.Finite,
});
export type JobResult = typeof JobResult.Type;
export const priorityWeight: Readonly<Record<Priority, number>> = { critical: 0, high: 1, normal: 2, low: 3 };
export const jobOrder = Order.mapInput(Order.Number, (job: Job) => priorityWeight[job.priority]);
export const executeJob = Effect.fn("PriorityQueue.executeJob")(function* (job: Job, queuePosition: number) {
  const startedAt = DateTime.formatIso(yield* DateTime.now);
  yield* Effect.logInfo(`[queue] #${queuePosition} ${job.priority} ${job.id}`);
  yield* Effect.sleep(Duration.millis(job.workMs));
  return JobResult.make({
    id: job.id,
    priority: job.priority,
    description: job.description,
    startedAt,
    completedAt: DateTime.formatIso(yield* DateTime.now),
    durationMs: job.workMs,
    queuePosition,
  });
});
