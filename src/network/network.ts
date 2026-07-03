import type { Stream } from "effect";
import { Context, Effect, Schema } from "effect";
import { TransportError } from "../Errors.ts";
import * as Protocol from "../Protocol.ts";

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

export const encodeRequest = <K extends Protocol.RequestKind>(request: Protocol.Request<K>): Effect.Effect<unknown> =>
  Effect.orDie(Schema.encodeUnknownEffect(Protocol.RequestFromWire)(request));

export const decodeResponse =
  <K extends Protocol.RequestKind>(request: Protocol.Request<K>) =>
  (input: unknown): Effect.Effect<Protocol.Response<K>, TransportError> =>
    Schema.decodeUnknownEffect(Protocol.ResponseSchemas[request.kind])(input).pipe(
      Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
      Effect.flatMap((response) => {
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
      }),
    );
