import { Effect, Schema, SchemaParser } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { bookCarRental, bookFlight, bookHotel, cancelFlight, cancelHotel } from "./services.ts";
export const repo = "example-saga-booking-ts";
export const functionName = "bookTrip";
export const sampleArgs = [{ tripId: "trip-1", shouldFail: false }] as const;
export const Payload = Schema.Struct({ tripId: Schema.String, shouldFail: Schema.Boolean });
export const BookingResult = Schema.Struct({
  status: Schema.Literals(["success", "failed"]),
  tripId: Schema.String,
  flightId: Schema.optional(Schema.String),
  hotelId: Schema.optional(Schema.String),
  carId: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
  compensated: Schema.optional(Schema.Array(Schema.String)),
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof BookingResult.Type, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        let flightId = "";
        let hotelId = "";
        const compensated: Array<string> = [];
        const booked = yield* Effect.gen(function* () {
          flightId = yield* ctx
            .run({ name: `book-flight-${input.tripId}`, effect: bookFlight(input.tripId) })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
          hotelId = yield* ctx
            .run({ name: `book-hotel-${input.tripId}`, effect: bookHotel(input.tripId) })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
          const carId = yield* ctx
            .run({
              name: `book-car-${input.tripId}`,
              effect: bookCarRental(input.tripId, input.shouldFail),
            })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
          return BookingResult.make({ status: "success", tripId: input.tripId, flightId, hotelId, carId });
        }).pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              if (hotelId.length > 0) {
                yield* ctx.run({ name: `cancel-hotel-${input.tripId}`, effect: cancelHotel(input.tripId, hotelId) });
                compensated.push("hotel");
              }
              if (flightId.length > 0) {
                yield* ctx.run({ name: `cancel-flight-${input.tripId}`, effect: cancelFlight(input.tripId, flightId) });
                compensated.push("flight");
              }
              return BookingResult.make({ status: "failed", tripId: input.tripId, error: String(error), compensated });
            }),
          ),
        );
        return booked;
      }),
  }),
);
