/**
 * Low-level client service for Resonate durable schedule protocol endpoints.
 *
 * Most applications define schedules through the high-level `Resonate.schedule`
 * API; this module exposes the underlying protocol service.
 *
 * @since 0.0.0
 */
import { Context, Crypto, Effect, Layer, SchemaParser } from "effect";
import { InvalidTarget, ScheduleNotFound, type ResonateProtocolError, type TransportError } from "./Errors.ts";
import { ResonateNetwork } from "./network/Network.ts";
import * as Protocol from "./Protocol.ts";

const isGetSuccess = SchemaParser.is(Protocol.ScheduleGetSuccessResponse);
const isCreateSuccess = SchemaParser.is(Protocol.ScheduleCreateSuccessResponse);
const isDeleteSuccess = SchemaParser.is(Protocol.ScheduleDeleteSuccessResponse);

const scheduleError = (options: {
  readonly id: Protocol.ScheduleId;
  readonly status: number;
  readonly message: unknown;
}): ResonateProtocolError => {
  const { id, status, message } = options;
  if (status === 404) {
    return new ScheduleNotFound({ id });
  }
  return new InvalidTarget({ message: String(message) });
};

/**
 * Service interface for schedule protocol operations.
 *
 * @category models
 * @since 0.0.0
 */
export interface SchedulesService {
  readonly get: (
    id: Protocol.ScheduleId,
  ) => Effect.Effect<Protocol.ScheduleRecord, ResonateProtocolError | TransportError>;
  readonly create: (
    data: Protocol.ScheduleCreateData,
  ) => Effect.Effect<Protocol.ScheduleRecord, ResonateProtocolError | TransportError>;
  readonly delete: (id: Protocol.ScheduleId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
}

/**
 * Protocol client service for durable schedules.
 *
 * @category services
 * @since 0.0.0
 */
export class Schedules extends Context.Service<Schedules, SchedulesService>()("effect-resonate/Schedules") {
  static readonly layer: Layer.Layer<Schedules, never, ResonateNetwork | Crypto.Crypto> = Layer.effect(
    Schedules,
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const crypto = yield* Crypto.Crypto;

      const head = Effect.fn("Schedules.head")(function* () {
        const corrId = Protocol.CorrelationId.make(yield* Effect.orDie(crypto.randomUUIDv4));
        return Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion });
      });

      return Schedules.of({
        get: Effect.fn("Schedules.get")(function* (id) {
          const response = yield* network.send(Protocol.ScheduleGetRequest.make({ head: yield* head(), data: { id } }));
          if (isGetSuccess(response)) {
            return response.data.schedule;
          }
          return yield* scheduleError({ id, status: response.head.status, message: response.data });
        }),
        create: Effect.fn("Schedules.create")(function* (data) {
          const response = yield* network.send(Protocol.ScheduleCreateRequest.make({ head: yield* head(), data }));
          if (isCreateSuccess(response)) {
            return response.data.schedule;
          }
          return yield* scheduleError({ id: data.id, status: response.head.status, message: response.data });
        }),
        delete: Effect.fn("Schedules.delete")(function* (id) {
          const response = yield* network.send(
            Protocol.ScheduleDeleteRequest.make({ head: yield* head(), data: { id } }),
          );
          if (isDeleteSuccess(response)) {
            return;
          }
          return yield* scheduleError({ id, status: response.head.status, message: response.data });
        }),
      });
    }),
  );
}
