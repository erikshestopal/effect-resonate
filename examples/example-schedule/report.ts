import { DateTime, Effect, Schema } from "effect";

export const ReportPayload = Schema.Finite;

export const generateReport = Effect.fn("generateReport")(function* (userId: number) {
  const timestamp = DateTime.formatIso(yield* DateTime.now);
  const report = `[${timestamp}] Report for user ${userId}`;
  yield* Effect.logInfo(report);
  return report;
});
