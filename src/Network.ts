/**
 * `ResonateNetwork` service interface (send + Stream recv) and the shared
 * envelope plumbing every transport uses.
 *
 * See `docs/DESIGN.md` §3.1 (Layer 1 — Transport: `ResonateNetwork`) and
 * `repos/resonate-sdk-ts/src/network/network.ts` (the native Send/Recv seam).
 *
 * Non-2xx protocol statuses are DATA, returned to layer 2 for interpretation.
 * Only genuine transport failures (connection loss, malformed frames, corrId
 * mismatch) become `TransportError` — with `401`/`403` mapped to the terminal
 * `Unauthorized` reason, never retried (DESIGN §6).
 */
import type { Stream } from "effect";
import { Context, Crypto, Effect, Schema } from "effect";
import { TransportError } from "./Errors.ts";
import * as Protocol from "./Protocol.ts";

export class ResonateNetwork extends Context.Service<
  ResonateNetwork,
  {
    /** SDK-initiated request/response. Correlation ids and protocol version handled here. */
    readonly send: <K extends Protocol.RequestKind>(
      request: Protocol.Request<K>,
    ) => Effect.Effect<Protocol.Response<K>, TransportError>;
    /** Server-pushed messages (execute / unblock) for this worker's addresses. */
    readonly messages: Stream.Stream<Protocol.Message, TransportError>;
    /** Translate a logical target into a structured transport address (`poll://any@group`). */
    readonly match: (target: Protocol.WorkerGroup) => Protocol.TargetAddress;
    readonly unicast: Protocol.TargetAddress;
    readonly anycast: (group: Protocol.WorkerGroup) => Protocol.TargetAddress;
  }
>()("effect-resonate/Network") {}

// -----------------------------------------------------------------------------
// Envelope helpers — shared by every transport, never reimplemented per transport
// -----------------------------------------------------------------------------

/** A fresh branded correlation id (UUIDv4, as the native `randomUUID()`). */
export const randomCorrelationId: Effect.Effect<Protocol.CorrelationId, never, Crypto.Crypto> = Effect.gen(
  function* () {
    const crypto = yield* Crypto.Crypto;
    const uuid = yield* Effect.orDie(crypto.randomUUIDv4);
    return Protocol.CorrelationId.make(uuid);
  },
);

/** A request head carrying a fresh corrId and the protocol version. */
export const makeRequestHead: Effect.Effect<Protocol.RequestHead, never, Crypto.Crypto> = Effect.map(
  randomCorrelationId,
  (corrId) => Protocol.RequestHead.make({ corrId, version: Protocol.protocolVersion }),
);

/**
 * Enforce the envelope rules on a decoded response: the corrId and kind must
 * match the request (else `CorrelationMismatch`), and `401`/`403` are terminal
 * `Unauthorized` transport failures.
 */
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

/** Encode a request for the wire. A validly constructed request always encodes. */
export const encodeRequest = <K extends Protocol.RequestKind>(request: Protocol.Request<K>): Effect.Effect<unknown> =>
  Effect.orDie(Schema.encodeUnknownEffect(Protocol.RequestFromWire)(request));

/**
 * Decode a wire response for the given request and enforce the envelope rules.
 * Frames that do not decode are `MalformedResponse` transport failures.
 */
export const decodeResponse =
  <K extends Protocol.RequestKind>(request: Protocol.Request<K>) =>
  (input: unknown): Effect.Effect<Protocol.Response<K>, TransportError> =>
    Schema.decodeUnknownEffect(Protocol.ResponseSchemas[request.kind])(input).pipe(
      Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
      Effect.flatMap(checkEnvelope(request)),
    );
