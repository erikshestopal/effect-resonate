import type { Stream } from "effect";
import { Context, Crypto, Effect, Schema } from "effect";
import { TransportError } from "./Errors.ts";
import * as Protocol from "./Protocol.ts";

export interface ResonateNetworkService {
  readonly send: <K extends Protocol.RequestKind>(
    request: Protocol.Request<K>,
  ) => Effect.Effect<Protocol.Response<K>, TransportError>;

  readonly messages: Stream.Stream<Protocol.Message, TransportError>;

  readonly match: (target: Protocol.WorkerGroup) => Protocol.TargetAddress;
  readonly unicast: Protocol.TargetAddress;
  readonly anycast: (group: Protocol.WorkerGroup) => Protocol.TargetAddress;
}

export class ResonateNetwork extends Context.Service<ResonateNetwork, ResonateNetworkService>()(
  "effect-resonate/Network",
) {}

export const randomCorrelationId: Effect.Effect<Protocol.CorrelationId, never, Crypto.Crypto> = Effect.gen(
  function* () {
    const crypto = yield* Crypto.Crypto;
    const uuid = yield* Effect.orDie(crypto.randomUUIDv4);
    return Protocol.CorrelationId.make(uuid);
  },
);

export const makeRequestHead: Effect.Effect<Protocol.RequestHead, never, Crypto.Crypto> = Effect.map(
  randomCorrelationId,
  (corrId) => Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion }),
);

export const checkEnvelope =
  <K extends Protocol.RequestKind>(request: Protocol.Request<K>) =>
  (response: Protocol.Response<K>): Effect.Effect<Protocol.Response<K>, TransportError> => {
    if (response.kind !== request.kind || response.head.corrId !== request.head.corrId) {
      return Effect.fail(
        new TransportError({
          reason: "CorrelationMismatch",
          cause: {
            expected: { kind: request.kind, corrId: request.head.corrId },
            received: { kind: response.kind, corrId: response.head.corrId },
          },
        }),
      );
    }
    if (response.head.status === 401 || response.head.status === 403) {
      return Effect.fail(new TransportError({ reason: "Unauthorized", cause: response }));
    }
    return Effect.succeed(response);
  };

export const encodeRequest = <K extends Protocol.RequestKind>(request: Protocol.Request<K>): Effect.Effect<unknown> =>
  Effect.orDie(Schema.encodeUnknownEffect(Protocol.RequestFromWire)(request));

export const decodeResponse =
  <K extends Protocol.RequestKind>(request: Protocol.Request<K>) =>
  (input: unknown): Effect.Effect<Protocol.Response<K>, TransportError> =>
    Schema.decodeUnknownEffect(Protocol.ResponseSchemas[request.kind])(input).pipe(
      Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
      Effect.flatMap(checkEnvelope(request)),
    );
