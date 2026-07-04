import { Effect, Match, Schema } from "effect";

export const UserEvent = Schema.Struct({
  eventId: Schema.String,
  type: Schema.Literals([
    "UserRegistered",
    "ProfileUpdated",
    "SubscriptionActivated",
    "OrderPlaced",
    "OrderShipped",
    "OrderCancelled",
    "SubscriptionRenewed",
  ]),
  payload: Schema.Record(Schema.String, Schema.Unknown),
});
export type UserEvent = typeof UserEvent.Type;

export const AccountProjection = Schema.Struct({
  userId: Schema.String,
  name: Schema.String,
  email: Schema.String,
  subscription: Schema.Literals(["none", "active", "cancelled"]),
  subscriptionRenewals: Schema.Finite,
  totalOrders: Schema.Finite,
  cancelledOrders: Schema.Finite,
  activeOrders: Schema.Array(Schema.String),
  shippedOrders: Schema.Array(Schema.String),
  eventsProcessed: Schema.Finite,
});
export type AccountProjection = typeof AccountProjection.Type;

export const initialProjection = (userId: string) =>
  AccountProjection.make({
    userId,
    name: "",
    email: "",
    subscription: "none",
    subscriptionRenewals: 0,
    totalOrders: 0,
    cancelledOrders: 0,
    activeOrders: [],
    shippedOrders: [],
    eventsProcessed: 0,
  });

const payloadString = (event: UserEvent, key: string) =>
  typeof event.payload[key] === "string" ? event.payload[key] : "";

export const project = (state: AccountProjection, event: UserEvent): AccountProjection =>
  Match.value(event.type).pipe(
    Match.when("UserRegistered", () =>
      AccountProjection.make({
        ...state,
        name: payloadString(event, "name"),
        email: payloadString(event, "email"),
        eventsProcessed: state.eventsProcessed + 1,
      }),
    ),
    Match.when("ProfileUpdated", () =>
      AccountProjection.make({
        ...state,
        name: payloadString(event, "name") || state.name,
        email: payloadString(event, "email") || state.email,
        eventsProcessed: state.eventsProcessed + 1,
      }),
    ),
    Match.when("SubscriptionActivated", () =>
      AccountProjection.make({ ...state, subscription: "active", eventsProcessed: state.eventsProcessed + 1 }),
    ),
    Match.when("OrderPlaced", () => {
      const orderId = payloadString(event, "orderId");
      return AccountProjection.make({
        ...state,
        totalOrders: state.totalOrders + 1,
        activeOrders: [...state.activeOrders, orderId],
        eventsProcessed: state.eventsProcessed + 1,
      });
    }),
    Match.when("OrderShipped", () => {
      const orderId = payloadString(event, "orderId");
      return AccountProjection.make({
        ...state,
        activeOrders: state.activeOrders.filter((id) => id !== orderId),
        shippedOrders: [...state.shippedOrders, orderId],
        eventsProcessed: state.eventsProcessed + 1,
      });
    }),
    Match.when("OrderCancelled", () => {
      const orderId = payloadString(event, "orderId");
      return AccountProjection.make({
        ...state,
        activeOrders: state.activeOrders.filter((id) => id !== orderId),
        cancelledOrders: state.cancelledOrders + 1,
        eventsProcessed: state.eventsProcessed + 1,
      });
    }),
    Match.when("SubscriptionRenewed", () =>
      AccountProjection.make({
        ...state,
        subscriptionRenewals: state.subscriptionRenewals + 1,
        eventsProcessed: state.eventsProcessed + 1,
      }),
    ),
    Match.exhaustive,
  );

export const applyEvent = Effect.fn("EventSourcing.applyEvent")(function* (
  eventIndex: number,
  event: UserEvent,
  state: AccountProjection,
) {
  yield* Effect.logInfo(`[event ${eventIndex}] ${event.type}`);
  return project(state, event);
});

export const makeSampleEvents = (userId: string): ReadonlyArray<UserEvent> => [
  UserEvent.make({
    eventId: "evt-1",
    type: "UserRegistered",
    payload: { userId, name: "Ada", email: "ada@example.com" },
  }),
  UserEvent.make({ eventId: "evt-2", type: "SubscriptionActivated", payload: {} }),
  UserEvent.make({ eventId: "evt-3", type: "OrderPlaced", payload: { orderId: "order-1" } }),
  UserEvent.make({ eventId: "evt-4", type: "OrderShipped", payload: { orderId: "order-1" } }),
];
