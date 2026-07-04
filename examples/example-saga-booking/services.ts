import { Duration, Effect, Schema } from "effect";

export const BookingConfirmation = Schema.String;
const booking = Effect.fn("SagaBooking.booking")(function* (kind: string, tripId: string) {
  yield* Effect.logInfo(`[${kind}] booking ${tripId}`);
  yield* Effect.sleep(Duration.millis(30));
  return `${kind.toUpperCase()}-${tripId}`;
});
export const bookFlight = (tripId: string) => booking("flight", tripId);
export const bookHotel = (tripId: string) => booking("hotel", tripId);
export const bookCarRental = Effect.fn("SagaBooking.bookCarRental")(function* (tripId: string, shouldFail: boolean) {
  yield* Effect.logInfo(`[car] booking ${tripId}`);
  yield* Effect.sleep(Duration.millis(30));
  if (shouldFail) return yield* Effect.fail("Car rental unavailable");
  return `CAR-${tripId}`;
});
export const cancelFlight = (tripId: string, confirmationId: string) =>
  Effect.logInfo(`[flight] cancel ${confirmationId} for ${tripId}`);
export const cancelHotel = (tripId: string, confirmationId: string) =>
  Effect.logInfo(`[hotel] cancel ${confirmationId} for ${tripId}`);
