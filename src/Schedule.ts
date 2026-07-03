import { Context, Crypto, Effect, Layer, SchemaParser } from "effect";
import { InvalidTarget, ScheduleNotFound, type ResonateProtocolError, type TransportError } from "./Errors.ts";
import { ResonateNetwork } from "./network/network.ts";
import * as Protocol from "./Protocol.ts";

const isGetSuccess = SchemaParser.is(Protocol.ScheduleGetResponse.members[0]);
const isCreateSuccess = SchemaParser.is(Protocol.ScheduleCreateResponse.members[0]);
const isDeleteSuccess = SchemaParser.is(Protocol.ScheduleDeleteResponse.members[0]);

const scheduleError = (id: Protocol.ScheduleId, status: number, message: unknown): ResonateProtocolError => {
  if (status === 404) {
    return new ScheduleNotFound({ id });
  }
  return new InvalidTarget({ message: String(message) });
};

export interface SchedulesService {
  readonly get: (
    id: Protocol.ScheduleId,
  ) => Effect.Effect<Protocol.ScheduleRecord, ResonateProtocolError | TransportError>;
  readonly create: (
    data: typeof Protocol.ScheduleCreateRequest.Type.data,
  ) => Effect.Effect<Protocol.ScheduleRecord, ResonateProtocolError | TransportError>;
  readonly delete: (id: Protocol.ScheduleId) => Effect.Effect<void, ResonateProtocolError | TransportError>;
}

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

      const get: SchedulesService["get"] = Effect.fn("Schedules.get")(function* (id) {
        const response = yield* network.send(Protocol.ScheduleGetRequest.make({ head: yield* head(), data: { id } }));
        if (isGetSuccess(response)) {
          return response.data.schedule;
        }
        return yield* Effect.fail(scheduleError(id, response.head.status, response.data));
      });

      const create: SchedulesService["create"] = Effect.fn("Schedules.create")(function* (data) {
        const response = yield* network.send(Protocol.ScheduleCreateRequest.make({ head: yield* head(), data }));
        if (isCreateSuccess(response)) {
          return response.data.schedule;
        }
        return yield* Effect.fail(scheduleError(data.id, response.head.status, response.data));
      });

      const deleteSchedule: SchedulesService["delete"] = Effect.fn("Schedules.delete")(function* (id) {
        const response = yield* network.send(
          Protocol.ScheduleDeleteRequest.make({ head: yield* head(), data: { id } }),
        );
        if (isDeleteSuccess(response)) {
          return;
        }
        return yield* Effect.fail(scheduleError(id, response.head.status, response.data));
      });

      return Schedules.of({ get, create, delete: deleteSchedule });
    }),
  );
}
