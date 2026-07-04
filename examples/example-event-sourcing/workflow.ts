import { Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { AccountProjection, UserEvent, applyEvent, initialProjection, makeSampleEvents } from "./events.ts";

export const repo = "example-event-sourcing-ts";
export const functionName = "processEventStream";
export const sampleArgs = [{ userId: "user-1", events: makeSampleEvents("user-1") }] as const;
export const Payload = Schema.Struct({ userId: Schema.String, events: Schema.Array(UserEvent) });
export const ProjectionResult = Schema.Struct({
  userId: Schema.String,
  eventsProcessed: Schema.Finite,
  finalProjection: AccountProjection,
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<
        typeof ProjectionResult.Type,
        unknown,
        ResonateContext.ResonateContext
      > {
        const ctx = yield* ResonateContext.ResonateContext;
        let projection = initialProjection(input.userId);
        for (let index = 0; index < input.events.length; index = index + 1) {
          const event = input.events[index]!;
          projection = yield* ctx
            .run({ name: `event-${event.eventId}`, effect: applyEvent(index, event, projection) })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(AccountProjection)));
        }
        return ProjectionResult.make({
          userId: input.userId,
          eventsProcessed: projection.eventsProcessed,
          finalProjection: projection,
        });
      }),
  }),
);
