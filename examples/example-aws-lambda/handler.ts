import { Effect, Schema, SchemaParser } from "effect";
import { DocumentJob, repo } from "./workflow.ts";
export const ApiGatewayEvent = Schema.Struct({
  httpMethod: Schema.String,
  path: Schema.String,
  body: Schema.NullOr(Schema.String),
  pathParameters: Schema.NullOr(Schema.Record(Schema.String, Schema.String)),
});
export const LambdaResponse = Schema.Struct({
  statusCode: Schema.Finite,
  headers: Schema.Record(Schema.String, Schema.String),
  body: Schema.String,
});
const headers = { "Content-Type": "application/json" };
const acceptedBody = (job: typeof DocumentJob.Type) =>
  `accepted ${job.jobId}; status=/status/${job.jobId}; repo=${repo}`;
export const handleProcessDocument = (input: unknown) =>
  SchemaParser.decodeUnknownEffect(DocumentJob)(input).pipe(
    Effect.map((job) =>
      LambdaResponse.make({
        statusCode: 202,
        headers,
        body: acceptedBody(job),
      }),
    ),
    Effect.orElseSucceed(() =>
      LambdaResponse.make({
        statusCode: 400,
        headers,
        body: "jobId, documentUrl, requesterId, and type are required",
      }),
    ),
  );
export const handler = (event: typeof ApiGatewayEvent.Type) =>
  Effect.gen(function* () {
    if (event.httpMethod === "POST" && event.path === "/process-document") {
      return yield* handleProcessDocument({});
    }
    return LambdaResponse.make({ statusCode: 404, headers, body: "Not found" });
  });
