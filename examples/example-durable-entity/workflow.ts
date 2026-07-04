import { Duration, Effect, Predicate, Schema } from "effect";
import { Resonate, ResonateContext, RetryPolicy } from "effect-resonate";
import {
  ActivityInput,
  SessionState,
  makeSessionOperations,
  type SessionState as SessionStateType,
} from "./session.ts";

export const repo = "example-durable-entity-ts";
export const functionName = "sessionLifecycle";

export const SessionLifecycleInput = Schema.Struct({
  sessionId: Schema.String,
  userId: Schema.String,
  activities: Schema.Array(ActivityInput),
  idleTimeoutMs: Schema.Finite,
  crashOnActivity: Schema.optional(Schema.NullOr(Schema.String)),
});
export type SessionLifecycleInput = typeof SessionLifecycleInput.Type;

export const SessionResult = Schema.Struct({
  sessionId: Schema.String,
  userId: Schema.String,
  finalStatus: Schema.String,
  activitiesRecorded: Schema.Finite,
  loginAt: Schema.String,
  expiredAt: Schema.optional(Schema.String),
  cleanedAt: Schema.optional(Schema.String),
});
export type SessionResult = typeof SessionResult.Type;

export const sampleArgs = [
  {
    sessionId: "sess_demo_1",
    userId: "user_alice_42",
    activities: [
      { type: "page_view", data: { path: "/products", referrer: "google.com" } },
      { type: "search", data: { query: "wireless headphones", results: 24 } },
      { type: "product_view", data: { productId: "prod_wh_001", name: "ANC Pro 5" } },
      { type: "add_to_cart", data: { productId: "prod_wh_001", quantity: 1, price: 149.99 } },
      { type: "checkout_started", data: { cartTotal: 149.99, itemCount: 1 } },
    ],
    idleTimeoutMs: 100,
    crashOnActivity: null,
  },
] as const;

export const workflow = Resonate.function({ name: functionName, payload: SessionLifecycleInput });
export const App = Resonate.group(workflow);

export const makeHandlers = Effect.fn("makeDurableEntityHandlers")(function* () {
  const operations = yield* makeSessionOperations();
  return App.toLayer(
    App.of({
      [functionName]: (input) =>
        Effect.gen(function* (): Effect.fn.Return<SessionResult, unknown, ResonateContext.ResonateContext> {
          const ctx = yield* ResonateContext.ResonateContext;
          let state: SessionStateType = yield* Schema.decodeUnknownEffect(SessionState)(
            yield* ctx.run({
              effect: operations.loginSession(input.sessionId, input.userId),
            }),
          );

          for (const activity of input.activities) {
            const shouldCrash = activity.type === input.crashOnActivity;
            state = yield* Schema.decodeUnknownEffect(SessionState)(
              yield* ctx.run({
                effect: operations.recordActivity(input.sessionId, state, activity, shouldCrash),
                options: { retryPolicy: RetryPolicy.constant({ delay: Duration.millis(10), maxRetries: 1 }) },
              }),
            );
          }

          state = yield* Schema.decodeUnknownEffect(SessionState)(
            yield* ctx.run({ effect: operations.markIdle(input.sessionId, state) }),
          );
          yield* ctx.sleep({ for: Duration.millis(input.idleTimeoutMs) });
          state = yield* Schema.decodeUnknownEffect(SessionState)(
            yield* ctx.run({ effect: operations.expireSession(input.sessionId, state) }),
          );
          state = yield* Schema.decodeUnknownEffect(SessionState)(
            yield* ctx.run({ effect: operations.cleanupSession(input.sessionId, state) }),
          );

          return SessionResult.make({
            sessionId: state.sessionId,
            userId: state.userId,
            finalStatus: state.status,
            activitiesRecorded: state.activities.length,
            loginAt: state.loginAt,
            ...(Predicate.isUndefined(state.expiredAt) ? {} : { expiredAt: state.expiredAt }),
            ...(Predicate.isUndefined(state.cleanedAt) ? {} : { cleanedAt: state.cleanedAt }),
          });
        }),
    }),
  );
});
