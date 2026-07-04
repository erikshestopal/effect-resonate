import { DateTime, Effect, Match, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
export const repo = "example-aws-lambda-ts";
export const functionName = "processDocument";
export const DocumentJob = Schema.Struct({
  jobId: Schema.String,
  documentUrl: Schema.String,
  requesterId: Schema.String,
  type: Schema.Literals(["invoice", "contract", "report"]),
});
export const DocumentResult = Schema.Struct({
  jobId: Schema.String,
  type: Schema.String,
  pageCount: Schema.Finite,
  summary: Schema.String,
  extractedData: Schema.Record(Schema.String, Schema.Unknown),
  storedAt: Schema.String,
  notifiedAt: Schema.String,
});
export const sampleArgs = [
  DocumentJob.make({
    jobId: "job-1",
    documentUrl: "s3://my-bucket/contracts/Q4-2025-agreement.pdf",
    requesterId: "user-alice",
    type: "contract",
  }),
] as const;
export const workflow = Resonate.function({ name: functionName, payload: DocumentJob });
export const App = Resonate.group(workflow);
const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
const downloadDocument = (job: typeof DocumentJob.Type) =>
  Effect.logInfo(`[download] ${job.jobId} fetching ${job.type} from ${job.documentUrl}`).pipe(
    Effect.as(job.type === "contract" ? 8 : 4),
  );
const extractText = (job: typeof DocumentJob.Type, pageCount: number) =>
  Effect.logInfo(`[extract] ${job.jobId} OCR on ${pageCount} pages`).pipe(
    Effect.as(`[text from ${pageCount}-page ${job.type}]`),
  );
const Analysis = Schema.Struct({
  summary: Schema.String,
  data: Schema.Record(Schema.String, Schema.Unknown),
});
const documentData = (kind: typeof DocumentJob.Type.type) =>
  Match.value(kind).pipe(
    Match.when("invoice", () => ({ vendor: "Acme Corp", amount: 4999, currency: "USD" })),
    Match.when("contract", () => ({ parties: ["Alice Inc.", "Bob LLC"], expires: "2027-01-01" })),
    Match.when("report", () => ({ period: "Q4 2025", metrics: { revenue: 1200000 } })),
    Match.exhaustive,
  );
const analyzeDocument = (job: typeof DocumentJob.Type, text: string) =>
  Effect.logInfo(`[analyze] ${job.jobId} analyzing ${job.type}`).pipe(
    Effect.as({
      summary: `${job.type} document processed. ${text.length} chars analyzed.`,
      data: documentData(job.type),
    }),
  );
const storeResults = (job: typeof DocumentJob.Type) =>
  Effect.logInfo(`[store] ${job.jobId} writing results`).pipe(Effect.andThen(now));
const notifyRequester = (job: typeof DocumentJob.Type) =>
  Effect.logInfo(`[notify] ${job.jobId} notifying ${job.requesterId}`).pipe(Effect.andThen(now));
export const handlers = App.toLayer(
  App.of({
    [functionName]: (job) =>
      Effect.gen(function* (): Effect.fn.Return<typeof DocumentResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const pageCount = yield* ctx
          .run({ name: "download", effect: downloadDocument(job) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.Finite)));
        const text = yield* ctx
          .run({ name: "extract", effect: extractText(job, pageCount) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        const analysis = yield* ctx
          .run({ name: "analyze", effect: analyzeDocument(job, text) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Analysis)));
        const storedAt = yield* ctx
          .run({ name: "store", effect: storeResults(job) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        const notifiedAt = yield* ctx
          .run({ name: "notify", effect: notifyRequester(job) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        return DocumentResult.make({
          jobId: job.jobId,
          type: job.type,
          pageCount,
          summary: analysis.summary,
          extractedData: analysis.data,
          storedAt,
          notifiedAt,
        });
      }),
  }),
);
