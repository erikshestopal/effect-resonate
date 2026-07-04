/**
 * Low-level client service for Resonate durable promise protocol endpoints.
 *
 * Most applications should use {@link Resonate.Client}; this module is
 * useful for implementing clients, workers, and protocol-level integrations.
 *
 * @since 0.0.0
 */
import { Context, Crypto, Duration, Effect, Filter, Layer, Option, Schedule, SchemaParser, Stream } from "effect";
import {
  InvalidTarget,
  PromiseNotFound,
  TaskFenced,
  type ResonateProtocolError,
  type TransportError,
} from "./Errors.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";

const isGetSuccess = SchemaParser.is(Protocol.PromiseGetSuccessResponse);
const isCreateSuccess = SchemaParser.is(Protocol.PromiseCreateSuccessResponse);
const isSettleSuccess = SchemaParser.is(Protocol.PromiseSettleSuccessResponse);
const isRegisterCallbackSuccess = SchemaParser.is(Protocol.PromiseRegisterCallbackSuccessResponse);
const isRegisterListenerSuccess = SchemaParser.is(Protocol.PromiseRegisterListenerSuccessResponse);

const promiseError = (options: {
  readonly id: Protocol.PromiseId;
  readonly status: number;
  readonly message: unknown;
}): ResonateProtocolError => {
  const { id, status, message } = options;
  if (status === 404) {
    return new PromiseNotFound({ id });
  }
  if (status === 409) {
    return new TaskFenced({ id, version: Protocol.TaskVersion.make(0) });
  }
  return new InvalidTarget({ message: String(message) });
};

/**
 * Service interface for durable promise operations.
 *
 * @category models
 * @since 0.0.0
 */
export interface DurablePromisesService {
  readonly get: (
    id: Protocol.PromiseId,
  ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly create: (
    data: Protocol.PromiseCreateData,
  ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly settle: (
    data: Protocol.PromiseSettleData,
  ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly registerCallback: (
    data: Protocol.PromiseRegisterCallbackData,
  ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly registerListener: (
    data: Protocol.PromiseRegisterListenerData,
  ) => Effect.Effect<Protocol.PromiseRecord, ResonateProtocolError | TransportError>;
  readonly awaitSettled: (
    id: Protocol.PromiseId,
  ) => Effect.Effect<Protocol.PromiseSettled, ResonateProtocolError | TransportError>;
}

/**
 * Protocol client service for durable promises.
 *
 * @category services
 * @since 0.0.0
 */
export class DurablePromises extends Context.Service<DurablePromises, DurablePromisesService>()(
  "effect-resonate/DurablePromises",
) {
  static readonly layer: Layer.Layer<DurablePromises, never, ResonateNetwork | Crypto.Crypto> = Layer.effect(
    DurablePromises,
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const crypto = yield* Crypto.Crypto;

      const head = Effect.fn("DurablePromises.head")(function* () {
        const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
        return Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion });
      });

      const service = DurablePromises.of({
        get: Effect.fn("DurablePromises.get")(function* (id) {
          const response = yield* network.send(Protocol.PromiseGetRequest.make({ head: yield* head(), data: { id } }));
          if (isGetSuccess(response)) {
            return response.data.promise;
          }
          return yield* promiseError({ id, status: response.head.status, message: response.data });
        }),
        create: Effect.fn("DurablePromises.create")(function* (data) {
          const response = yield* network.send(Protocol.PromiseCreateRequest.make({ head: yield* head(), data }));
          if (isCreateSuccess(response)) {
            return response.data.promise;
          }
          return yield* promiseError({ id: data.id, status: response.head.status, message: response.data });
        }),
        settle: Effect.fn("DurablePromises.settle")(function* (data) {
          const response = yield* network.send(Protocol.PromiseSettleRequest.make({ head: yield* head(), data }));
          if (isSettleSuccess(response)) {
            return response.data.promise;
          }
          return yield* promiseError({ id: data.id, status: response.head.status, message: response.data });
        }),
        registerCallback: Effect.fn("DurablePromises.registerCallback")(function* (data) {
          const response = yield* network.send(
            Protocol.PromiseRegisterCallbackRequest.make({ head: yield* head(), data }),
          );
          if (isRegisterCallbackSuccess(response)) {
            return response.data.promise;
          }
          return yield* promiseError({ id: data.awaited, status: response.head.status, message: response.data });
        }),
        registerListener: Effect.fn("DurablePromises.registerListener")(function* (data) {
          const response = yield* network.send(
            Protocol.PromiseRegisterListenerRequest.make({ head: yield* head(), data }),
          );
          if (isRegisterListenerSuccess(response)) {
            return response.data.promise;
          }
          return yield* promiseError({ id: data.awaited, status: response.head.status, message: response.data });
        }),
        awaitSettled: Effect.fn("DurablePromises.awaitSettled")(function* (id) {
          const observed = yield* Effect.gen(function* () {
            const promise = yield* service.registerListener(
              Protocol.PromiseRegisterListenerData.make({ awaited: id, address: network.unicast }),
            );
            if (promise.state !== "pending") {
              return Option.some(promise);
            }
            return yield* Stream.runHead(
              network.messages.pipe(
                Stream.filterMap(
                  Filter.fromPredicateOption((message: Protocol.Message): Option.Option<Protocol.PromiseSettled> => {
                    if (message.kind !== "unblock" || message.data.promise.id !== id) {
                      return Option.none();
                    }
                    if (message.data.promise.state === "pending") {
                      return Option.none();
                    }
                    return Option.some(message.data.promise);
                  }),
                ),
                Stream.take(1),
              ),
            ).pipe(Effect.race(Effect.as(Effect.sleep(Duration.seconds(60)), Option.none<Protocol.PromiseSettled>())));
          }).pipe(Effect.repeat({ until: Option.isSome }), Effect.retry(Schedule.spaced(Duration.seconds(5))));
          return yield* Option.match(observed, {
            onNone: () => Effect.die("DurablePromises.awaitSettled repeated without observing settlement"),
            onSome: Effect.succeed,
          });
        }),
      });

      return service;
    }),
  );
}
