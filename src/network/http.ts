/**
 * HTTP implementation of the Resonate network service.
 *
 * The layer uses Effect's abstract `HttpClient` service and therefore stays
 * independent of Bun, Node, or any specific runtime implementation.
 *
 * @since 0.0.0
 */
import { Duration, Effect, Filter, HashSet, Layer, Option, Schedule, Schema, Stream, String as Str } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { TransportError } from "../Errors.ts";
import * as Protocol from "../Protocol.ts";
import { decodeResponse, encodeRequest, ResonateNetwork } from "./network.ts";

/**
 * Options for connecting to a Resonate server over HTTP.
 *
 * @category models
 * @since 0.0.0
 */
export interface NetworkHttpOptions {
  readonly url: string;
  readonly group?: string;
  readonly pid?: string;
  readonly token?: string;
}

const protocolStatuses = HashSet.make(200, 300, 404, 409, 422, 501);

/**
 * Builds a network service backed by the Resonate HTTP API and SSE stream.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (options: NetworkHttpOptions): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> =>
  Layer.effect(
    ResonateNetwork,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const group = Protocol.WorkerGroup.make(options.group ?? "default");
      const pid = Protocol.ProcessId.make(options.pid ?? "local");
      const configuredPid = Option.as(Option.fromNullishOr(options.pid), pid);
      const token = Option.fromNullishOr(options.token);
      const baseUrl = Str.endsWith("/")(options.url) ? Str.slice(0, -1)(options.url) : options.url;
      const commonHeaders = Option.match(token, {
        onNone: () => ({ "Content-Type": "application/json" }),
        onSome: (token) => ({ "Content-Type": "application/json", Authorization: `Bearer ${token}` }),
      });

      const send = Effect.fn("NetworkHttp.send")(function* <K extends Protocol.RequestKind>(
        request: Protocol.Request<K>,
      ) {
        const requestWithAuth = Option.match(token, {
          onNone: () => request,
          onSome: (auth) => ({ ...request, head: Protocol.RequestHead.make({ ...request.head, auth }) }),
        });
        const wire = yield* encodeRequest(requestWithAuth);
        const httpRequest = yield* HttpClientRequest.post(options.url, { headers: commonHeaders }).pipe(
          HttpClientRequest.bodyJson(wire),
          Effect.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })),
        );
        const response = yield* client
          .execute(httpRequest)
          .pipe(Effect.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })));
        if (response.status === 401 || response.status === 403) {
          return yield* new TransportError({ reason: "Unauthorized", cause: response.status });
        }
        if (!HashSet.has(protocolStatuses, response.status)) {
          return yield* new TransportError({ reason: "ConnectionLost", cause: response.status });
        }
        const body = yield* response.json.pipe(
          Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
        );
        return yield* decodeResponse(request)(body);
      });

      const connectMessages = Stream.unwrap(
        client
          .get(`${baseUrl}${Protocol.TargetAddress.pollUni({ group, id: pid }).pollPath}`, {
            headers: commonHeaders,
            accept: "text/event-stream",
          })
          .pipe(
            Effect.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })),
            Effect.flatMap((response) =>
              response.status === 401 || response.status === 403
                ? new TransportError({ reason: "Unauthorized", cause: response.status })
                : response.status !== 200
                  ? new TransportError({ reason: "ConnectionLost", cause: response.status })
                  : Effect.succeed(response),
            ),
            Effect.map((response) =>
              response.stream.pipe(
                Stream.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })),
                Stream.decodeText(),
                Stream.splitLines,
                Stream.filterMap(
                  Filter.fromPredicateOption((line) =>
                    Str.startsWith("data:")(line)
                      ? Option.some(Str.trimStart(Str.slice("data:".length)(line)))
                      : Option.none(),
                  ),
                ),
                Stream.mapEffect((input) =>
                  Schema.decodeUnknownEffect(Schema.fromJsonString(Protocol.Message))(input).pipe(
                    Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
                  ),
                ),
              ),
            ),
          ),
      );

      const reconnect: Schedule.Schedule<Duration.Duration, TransportError> = Schedule.exponential(
        Duration.seconds(1),
      ).pipe(Schedule.modifyDelay((_, delay) => Effect.succeed(Duration.min(delay, Duration.seconds(30)))));
      const retryReconnect = reconnect.pipe(Schedule.while(({ input }) => input.reason !== "Unauthorized"));
      const messagesPubSub = yield* connectMessages.pipe(
        Stream.retry(retryReconnect),
        Stream.toPubSubTake({ capacity: "unbounded", replay: 1 }),
      );

      return ResonateNetwork.of({
        send,
        messages: Stream.fromPubSubTake(messagesPubSub),
        match: (target) => Protocol.TargetAddress.pollAny({ group: target }),
        unicast: Protocol.TargetAddress.pollUni({ group, id: pid }),
        anycast: (target) => Protocol.TargetAddress.pollAny({ group: target, id: configuredPid }),
      });
    }),
  );
