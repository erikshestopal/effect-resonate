/**
 * Promise domain model + typed client ops.
 *
 * See `docs/DESIGN.md` §3.2 (Layer 2 — Protocol client).
 */
import { Context, Crypto, Duration, Effect, Filter, Layer, Option, Schedule, SchemaParser, Stream } from "effect";
import {
  InvalidTarget,
  PromiseNotFound,
  TaskFenced,
  type ResonateProtocolError,
  type TransportError,
} from "./Errors.ts";
import { ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";

const isGetSuccess = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);
const isCreateSuccess = SchemaParser.is(Protocol.PromiseCreateResponse.members[0]);
const isSettleSuccess = SchemaParser.is(Protocol.PromiseSettleResponse.members[0]);
const isRegisterCallbackSuccess = SchemaParser.is(Protocol.PromiseRegisterCallbackResponse.members[0]);
const isRegisterListenerSuccess = SchemaParser.is(Protocol.PromiseRegisterListenerResponse.members[0]);

const promiseError = (id: Protocol.PromiseId, status: number, message: unknown): ResonateProtocolError => {
  if (status === 404) {
    return new PromiseNotFound({ id });
  }
  if (status === 409) {
    return new TaskFenced({ id, version: Protocol.TaskVersion.make(0) });
  }
  return new InvalidTarget({ message: String(message) });
};

export class DurablePromises extends Context.Service<
  DurablePromises,
  {
    readonly get: (
      id: Protocol.PromiseId,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly create: (
      data: typeof Protocol.PromiseCreateRequest.Type.data,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly settle: (
      data: typeof Protocol.PromiseSettleRequest.Type.data,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly registerCallback: (
      data: typeof Protocol.PromiseRegisterCallbackRequest.Type.data,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly registerListener: (
      data: typeof Protocol.PromiseRegisterListenerRequest.Type.data,
    ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
    readonly awaitSettled: (
      id: Protocol.PromiseId,
    ) => Effect.Effect<Protocol.PromiseSettled, ResonateProtocolError | TransportError>;
  }
>()("effect-resonate/DurablePromises") {
  static readonly layer: Layer.Layer<DurablePromises, never, ResonateNetwork | Crypto.Crypto> = Layer.effect(
    DurablePromises,
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const crypto = yield* Crypto.Crypto;

      const head = Effect.fn("DurablePromises.head")(function* () {
        const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
        return Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion });
      });

      const get = Effect.fn("DurablePromises.get")(function* (id: Protocol.PromiseId) {
        const response = yield* network.send(Protocol.PromiseGetRequest.make({ head: yield* head(), data: { id } }));
        if (isGetSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(promiseError(id, response.head.status, response.data));
      });

      const create = Effect.fn("DurablePromises.create")(function* (
        data: typeof Protocol.PromiseCreateRequest.Type.data,
      ) {
        const response = yield* network.send(Protocol.PromiseCreateRequest.make({ head: yield* head(), data }));
        if (isCreateSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(promiseError(data.id, response.head.status, response.data));
      });

      const settle = Effect.fn("DurablePromises.settle")(function* (
        data: typeof Protocol.PromiseSettleRequest.Type.data,
      ) {
        const response = yield* network.send(Protocol.PromiseSettleRequest.make({ head: yield* head(), data }));
        if (isSettleSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(promiseError(data.id, response.head.status, response.data));
      });

      const registerCallback = Effect.fn("DurablePromises.registerCallback")(function* (
        data: typeof Protocol.PromiseRegisterCallbackRequest.Type.data,
      ) {
        const response = yield* network.send(
          Protocol.PromiseRegisterCallbackRequest.make({ head: yield* head(), data }),
        );
        if (isRegisterCallbackSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(promiseError(data.awaited, response.head.status, response.data));
      });

      const registerListener = Effect.fn("DurablePromises.registerListener")(function* (
        data: typeof Protocol.PromiseRegisterListenerRequest.Type.data,
      ) {
        const response = yield* network.send(
          Protocol.PromiseRegisterListenerRequest.make({ head: yield* head(), data }),
        );
        if (isRegisterListenerSuccess(response)) {
          return response.data.promise;
        }
        return yield* Effect.fail(promiseError(data.awaited, response.head.status, response.data));
      });

      const settledFor = (id: Protocol.PromiseId) =>
        Filter.fromPredicateOption((message: Protocol.Message): Option.Option<Protocol.PromiseSettled> => {
          if (message.kind !== "unblock" || message.data.promise.id !== id) {
            return Option.none();
          }
          if (message.data.promise.state === "pending") {
            return Option.none();
          }
          return Option.some(message.data.promise);
        });

      const awaitSettled = Effect.fn("DurablePromises.awaitSettled")(function* (id: Protocol.PromiseId) {
        const loop: Effect.Effect<Protocol.PromiseSettled, ResonateProtocolError | TransportError> = Effect.suspend(
          () =>
            Effect.gen(function* () {
              const promise = yield* registerListener({ awaited: id, address: network.unicast });
              if (promise.state !== "pending") {
                return promise;
              }
              const observed = yield* Stream.runHead(
                network.messages.pipe(Stream.filterMap(settledFor(id)), Stream.take(1)),
              ).pipe(
                Effect.catchCause(() =>
                  Effect.as(Effect.sleep(Duration.seconds(5)), Option.none<Protocol.PromiseSettled>()),
                ),
                Effect.race(Effect.as(Effect.sleep(Duration.seconds(60)), Option.none<Protocol.PromiseSettled>())),
              );
              return yield* Option.match(observed, {
                onNone: () => loop,
                onSome: Effect.succeed,
              });
            }),
        );
        return yield* loop.pipe(Effect.retry(Schedule.spaced(Duration.seconds(5))));
      });

      return DurablePromises.of({ get, create, settle, registerCallback, registerListener, awaitSettled });
    }),
  );
}
