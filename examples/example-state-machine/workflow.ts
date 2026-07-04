import { Effect, Match, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
import { OrderState, Transition, transitionTo } from "./transitions.ts";
export const repo = "example-state-machine-ts";
export const functionName = "orderLifecycle";
export const sampleArgs = [{ orderId: "order-1", path: "deliver" }] as const;
export const Payload = Schema.Struct({ orderId: Schema.String, path: Schema.Literals(["deliver", "cancel", "crash"]) });
export const OrderResult = Schema.Struct({
  orderId: Schema.String,
  finalState: OrderState,
  history: Schema.Array(Transition),
});
export const workflow = Resonate.function({ name: functionName, payload: Payload });
export const App = Resonate.group(workflow);
export const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<typeof OrderResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        const history: Array<Transition> = [];
        const step = (from: OrderState | null, to: OrderState) =>
          ctx
            .run({ name: `${input.orderId}-${to}`, effect: transitionTo(input.orderId, from, to) })
            .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Transition)));
        history.push(yield* step(null, "created"));
        history.push(yield* step("created", "confirmed"));
        return yield* Match.value(input.path).pipe(
          Match.when("cancel", () =>
            Effect.gen(function* () {
              history.push(yield* step("confirmed", "cancelled"));
              history.push(yield* step("cancelled", "refunded"));
              return OrderResult.make({ orderId: input.orderId, finalState: "refunded", history });
            }),
          ),
          Match.whenOr("deliver", "crash", () =>
            Effect.gen(function* () {
              history.push(yield* step("confirmed", "shipped"));
              history.push(yield* step("shipped", "delivered"));
              return OrderResult.make({ orderId: input.orderId, finalState: "delivered", history });
            }),
          ),
          Match.exhaustive,
        );
      }),
  }),
);
