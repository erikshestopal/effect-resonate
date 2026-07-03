/**
 * Runtime-neutral network abstraction for the Resonate protocol.
 *
 * Implementations send request/response protocol messages and expose the worker
 * message stream used by {@link Worker.layer}. HTTP and local in-memory
 * implementations both provide this service.
 *
 * @since 0.0.0
 */
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

/**
 * Service for sending protocol requests and consuming worker messages.
 *
 * @category services
 * @since 0.0.0
 */
export class ResonateNetwork extends Context.Service<ResonateNetwork, ResonateNetworkService>()(
  "effect-resonate/Network",
) {}

/**
 * Encodes a protocol request to its wire shape.
 *
 * @category encoding
 * @since 0.0.0
 */
export const encodeRequest = <K extends Protocol.RequestKind>(request: Protocol.Request<K>): Effect.Effect<unknown> =>
  Effect.orDie(Schema.encodeUnknownEffect(Protocol.RequestFromWire)(request));

/**
 * Decodes and validates a response for the matching request envelope.
 *
 * @category encoding
 * @since 0.0.0
 */
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
