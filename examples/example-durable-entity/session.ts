import { DateTime, Duration, Effect, HashMap, Option, Ref, Schema } from "effect";

export const SessionStatus = Schema.Literals(["active", "idle", "expired", "cleaned_up"]);
export type SessionStatus = typeof SessionStatus.Type;

export const ActivityInput = Schema.Struct({
  type: Schema.String,
  data: Schema.Record(Schema.String, Schema.Any),
});
export type ActivityInput = typeof ActivityInput.Type;

export const Activity = Schema.Struct({
  type: Schema.String,
  data: Schema.Record(Schema.String, Schema.Any),
  recordedAt: Schema.String,
});
export type Activity = typeof Activity.Type;

export const SessionState = Schema.Struct({
  sessionId: Schema.String,
  userId: Schema.String,
  status: SessionStatus,
  loginAt: Schema.String,
  activities: Schema.Array(Activity),
  expiredAt: Schema.optional(Schema.String),
  cleanedAt: Schema.optional(Schema.String),
});
export type SessionState = typeof SessionState.Type;

export class ActivityWriteTimedOut extends Schema.TaggedErrorClass<ActivityWriteTimedOut>()("ActivityWriteTimedOut", {
  sessionId: Schema.String,
  activityType: Schema.String,
}) {}

export interface SessionOperations {
  readonly loginSession: (sessionId: string, userId: string) => Effect.Effect<SessionState>;
  readonly recordActivity: (
    sessionId: string,
    state: SessionState,
    activity: ActivityInput,
    shouldCrash: boolean,
  ) => Effect.Effect<SessionState, ActivityWriteTimedOut>;
  readonly markIdle: (sessionId: string, state: SessionState) => Effect.Effect<SessionState>;
  readonly expireSession: (sessionId: string, state: SessionState) => Effect.Effect<SessionState>;
  readonly cleanupSession: (sessionId: string, state: SessionState) => Effect.Effect<SessionState>;
}

export const makeSessionOperations = Effect.fn("makeSessionOperations")(function* () {
  const attempts = yield* Ref.make(HashMap.empty<string, number>());
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

  const loginSession = Effect.fn("loginSession")(function* (sessionId: string, userId: string) {
    yield* Effect.sleep(Duration.millis(30));
    const state = SessionState.make({
      sessionId,
      userId,
      status: "active",
      loginAt: yield* nowIso,
      activities: [],
    });
    yield* Effect.logInfo(`[${sessionId}] User ${userId} logged in`);
    return state;
  });

  const recordActivity = Effect.fn("recordActivity")(function* (
    sessionId: string,
    state: SessionState,
    activity: ActivityInput,
    shouldCrash: boolean,
  ): Effect.fn.Return<SessionState, ActivityWriteTimedOut> {
    const key = `${sessionId}:activity:${activity.type}`;
    const attempt = yield* Ref.modify(attempts, (current) => {
      const nextAttempt = Option.getOrElse(HashMap.get(current, key), () => 0) + 1;
      return [nextAttempt, HashMap.set(current, key, nextAttempt)] as const;
    });

    yield* Effect.sleep(Duration.millis(40));

    if (shouldCrash && attempt === 1) {
      yield* Effect.logWarning(`[${sessionId}] Activity '${activity.type}' failed with database write timeout`);
      return yield* new ActivityWriteTimedOut({ sessionId, activityType: activity.type });
    }

    const recorded = Activity.make({
      type: activity.type,
      data: activity.data,
      recordedAt: yield* nowIso,
    });
    const retryTag = attempt > 1 ? ` retry ${attempt}` : "";
    yield* Effect.logInfo(`[${sessionId}] Activity '${activity.type}' recorded${retryTag}`);
    return SessionState.make({ ...state, activities: [...state.activities, recorded] });
  });

  const markIdle = Effect.fn("markIdle")(function* (sessionId: string, state: SessionState) {
    yield* Effect.logInfo(`[${sessionId}] Session idle; waiting for durable timeout`);
    return SessionState.make({ ...state, status: "idle" });
  });

  const expireSession = Effect.fn("expireSession")(function* (sessionId: string, state: SessionState) {
    yield* Effect.sleep(Duration.millis(20));
    yield* Effect.logInfo(`[${sessionId}] Session expired after idle timeout`);
    return SessionState.make({ ...state, status: "expired", expiredAt: yield* nowIso });
  });

  const cleanupSession = Effect.fn("cleanupSession")(function* (sessionId: string, state: SessionState) {
    yield* Effect.sleep(Duration.millis(50));
    yield* Effect.logInfo(`[${sessionId}] Session cleaned up; tokens revoked and cache cleared`);
    return SessionState.make({ ...state, status: "cleaned_up", cleanedAt: yield* nowIso });
  });

  return {
    loginSession,
    recordActivity,
    markIdle,
    expireSession,
    cleanupSession,
  } satisfies SessionOperations;
});
