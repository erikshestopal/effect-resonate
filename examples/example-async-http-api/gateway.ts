import { Clock, Effect, Exit, Match, Option, Schema } from "effect";
import { Protocol, Resonate } from "effect-resonate";
import { functionName, workflow } from "./workflow.ts";

export const BeginRequest = Schema.Struct({
  queryId: Schema.optionalKey(Schema.String),
  body: Schema.optionalKey(Schema.Unknown),
});

export const BeginResponse = Schema.Struct({
  promise: Schema.String,
  status: Schema.Literal("pending"),
  wait: Schema.String,
});

export const WaitRequest = Schema.Struct({
  id: Schema.String,
});

export const WaitResponse = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("pending"),
    promise_id: Schema.String,
    message: Schema.String,
  }),
  Schema.Struct({
    status: Schema.Literal("resolved"),
    promise_id: Schema.String,
    result: Schema.Unknown,
  }),
  Schema.Struct({
    status: Schema.Literal("rejected"),
    promise_id: Schema.String,
    error: Schema.String,
  }),
]);

const defaultBody = { foo: "bar" };

const requestId = Effect.gen(function* () {
  const millis = yield* Clock.currentTimeMillis;
  return Protocol.ExecutionId.make(`async-http-api-${millis}`);
});

export const begin = Effect.fn("AsyncHttpApiGateway.begin")(function* (
  request: typeof BeginRequest.Type,
): Effect.fn.Return<typeof BeginResponse.Type, unknown, Resonate.Client> {
  const client = yield* Resonate.Client;
  const executionId = yield* Option.fromNullishOr(request.queryId).pipe(
    Option.match({
      onNone: () => requestId,
      onSome: (id) => Effect.succeed(Protocol.ExecutionId.make(id)),
    }),
  );
  const handle = yield* client.beginRpc({
    targetFunction: workflow,
    executionId,
    args: [Option.getOrElse(Option.fromNullishOr(request.body), () => defaultBody)],
    options: { target: Protocol.WorkerGroup.make("worker") },
  });
  return BeginResponse.make({
    promise: handle.id,
    status: "pending",
    wait: `/wait?id=${handle.id}`,
  });
});

export const wait = Effect.fn("AsyncHttpApiGateway.wait")(function* (
  request: typeof WaitRequest.Type,
): Effect.fn.Return<typeof WaitResponse.Type, unknown, Resonate.Client> {
  const client = yield* Resonate.Client;
  const handle = yield* client.get({ fn: workflow, id: Protocol.ExecutionId.make(request.id) });
  const polled = yield* handle.poll;
  return yield* Option.match(polled, {
    onNone: () =>
      Effect.succeed(
        WaitResponse.make({
          status: "pending",
          promise_id: request.id,
          message: "Processing in progress",
        }),
      ),
    onSome: (exit) =>
      Match.value(exit).pipe(
        Match.when(Exit.isSuccess, (exit) =>
          Effect.succeed(
            WaitResponse.make({
              status: "resolved",
              promise_id: request.id,
              result: exit.value,
            }),
          ),
        ),
        Match.when(Exit.isFailure, (exit) =>
          Effect.succeed(
            WaitResponse.make({
              status: "rejected",
              promise_id: request.id,
              error: String(exit.cause),
            }),
          ),
        ),
        Match.exhaustive,
      ),
  });
});

export const gatewayRoutes = {
  begin: { method: "POST", path: "/begin", handler: begin },
  wait: { method: "GET", path: "/wait", handler: wait },
  workerFunction: functionName,
} as const;
