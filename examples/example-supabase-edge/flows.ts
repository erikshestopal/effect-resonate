import { Effect, Schema, SchemaParser } from "effect";
import { UserRecord, functionName } from "./workflow.ts";
export const DatabaseWebhook = Schema.Struct({
  type: Schema.String,
  table: Schema.String,
  schema: Schema.String,
  record: UserRecord,
});
export const handleWebhook = (input: unknown) =>
  SchemaParser.decodeUnknownEffect(DatabaseWebhook)(input).pipe(
    Effect.map((payload) => ({
      status: payload.type === "INSERT" && payload.table === "users" ? "ok" : "ignored",
      promiseId: `onboard/${payload.record.id}`,
      func: functionName,
      args: [payload.record],
    })),
  );
