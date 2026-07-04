import { Array as Arr, Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import { Job, JobResult, executeJob, jobOrder } from "./jobs.ts";
export const repo = "example-priority-queue-ts";
export const functionName = "processQueue";
export const sampleArgs = [
  {
    jobs: [
      Job.make({ id: "job-1", priority: "critical", description: "ship", workMs: 1 }),
      Job.make({ id: "job-2", priority: "low", description: "archive", workMs: 1 }),
    ],
  },
] as const;
export const Payload = Schema.Struct({ jobs: Schema.Array(Job) });
export const QueueResult = Schema.Struct({
  totalJobs: Schema.Finite,
  completedJobs: Schema.Finite,
  processingOrder: Schema.Array(Schema.String),
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof QueueResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const results: Array<JobResult> = [];
        const sorted = Arr.sort(input.jobs, jobOrder);
        for (let index = 0; index < sorted.length; index = index + 1) {
          const job = sorted[index]!;
          results.push(
            yield* ctx
              .run({ name: `job-${job.id}`, effect: executeJob(job, index + 1) })
              .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(JobResult))),
          );
        }
        return QueueResult.make({
          totalJobs: input.jobs.length,
          completedJobs: results.length,
          processingOrder: results.map((result) => `${result.id} [${result.priority}]`),
        });
      }),
  }),
);
