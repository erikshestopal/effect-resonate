/**
 * HTTP POST + SSE poll transport.
 *
 * See `docs/DESIGN.md` §3.1 (Layer 1 — Transport: `ResonateNetwork`), `NetworkHttp.layer`.
 */
import { Duration, Effect, Filter, Layer, Option, Predicate, Schedule, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { TransportError } from "./Errors.ts";
import { decodeResponse, encodeRequest, ResonateNetwork } from "./Network.ts";
import * as Protocol from "./Protocol.ts";

export interface NetworkHttpOptions {
  readonly url: string;
  readonly group?: string;
  readonly pid?: string;
  readonly token?: string;
}

const protocolStatuses = new Set([200, 300, 404, 409, 422, 501]);

const connectionLost = (cause: unknown) => new TransportError({ reason: "ConnectionLost", cause });
const malformedResponse = (cause: unknown) => new TransportError({ reason: "MalformedResponse", cause });

const headers = (token: Option.Option<string>) =>
  Option.match(token, {
    onNone: () => ({ "Content-Type": "application/json" }),
    onSome: (token) => ({ "Content-Type": "application/json", Authorization: `Bearer ${token}` }),
  });

const trimTrailingSlash = (value: string): string => (value.endsWith("/") ? value.slice(0, -1) : value);

const pollUrl = (base: string, group: Protocol.WorkerGroup, pid: Protocol.ProcessId): string =>
  `${trimTrailingSlash(base)}/poll/${encodeURIComponent(group)}/${encodeURIComponent(pid)}`;

const decodeMessageJson = (input: string): Effect.Effect<Protocol.Message, TransportError> =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(Protocol.Message))(input).pipe(Effect.mapError(malformedResponse));

const decodeSseLine = (line: string): Option.Option<string> => {
  if (!line.startsWith("data:")) {
    return Option.none();
  }
  return Option.some(line.slice("data:".length).trimStart());
};

export const layer = (options: NetworkHttpOptions): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> =>
  Layer.effect(
    ResonateNetwork,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const group = Protocol.WorkerGroup.make(options.group ?? "default");
      const pid = Protocol.ProcessId.make(options.pid ?? "local");
      const token = Option.fromNullishOr(options.token);
      const commonHeaders = headers(token);

      const send = Effect.fn("NetworkHttp.send")(function* <K extends Protocol.RequestKind>(
        request: Protocol.Request<K>,
      ) {
        const wire = yield* encodeRequest(request);
        const httpRequest = yield* HttpClientRequest.post(options.url, { headers: commonHeaders }).pipe(
          HttpClientRequest.bodyJson(wire),
          Effect.mapError(connectionLost),
        );
        const response = yield* client.execute(httpRequest).pipe(Effect.mapError(connectionLost));
        if (response.status === 401 || response.status === 403) {
          return yield* new TransportError({ reason: "Unauthorized", cause: response.status });
        }
        if (!protocolStatuses.has(response.status)) {
          return yield* connectionLost(response.status);
        }
        const body = yield* response.json.pipe(Effect.mapError(malformedResponse));
        return yield* decodeResponse(request)(body);
      });

      const connectMessages = Stream.unwrap(
        client.get(pollUrl(options.url, group, pid), { headers: commonHeaders, accept: "text/event-stream" }).pipe(
          Effect.mapError(connectionLost),
          Effect.flatMap((response) =>
            response.status === 401 || response.status === 403
              ? Effect.fail(new TransportError({ reason: "Unauthorized", cause: response.status }))
              : response.status !== 200
                ? Effect.fail(connectionLost(response.status))
                : Effect.succeed(response),
          ),
          Effect.map((response) =>
            response.stream.pipe(
              Stream.mapError(connectionLost),
              Stream.decodeText(),
              Stream.splitLines,
              Stream.filterMap(Filter.fromPredicateOption(decodeSseLine)),
              Stream.mapEffect(decodeMessageJson),
            ),
          ),
        ),
      );

      const reconnect: Schedule.Schedule<Duration.Duration, TransportError> = Schedule.exponential(
        Duration.seconds(1),
      ).pipe(Schedule.modifyDelay((_, delay) => Effect.succeed(Duration.min(delay, Duration.seconds(30)))));
      const retryReconnect = reconnect.pipe(Schedule.while(({ input }) => input.reason !== "Unauthorized"));

      return ResonateNetwork.of({
        send,
        messages: connectMessages.pipe(Stream.retry(retryReconnect)),
        match: (target) =>
          Protocol.TargetAddress.make({
            transport: "poll",
            cast: "any",
            group: target,
            id: Option.none(),
          }),
        unicast: Protocol.TargetAddress.make({
          transport: "poll",
          cast: "uni",
          group,
          id: Option.some(pid),
        }),
        anycast: (target) =>
          Protocol.TargetAddress.make({
            transport: "poll",
            cast: "any",
            group: target,
            id: Predicate.isUndefined(options.pid) ? Option.none() : Option.some(pid),
          }),
      });
    }),
  );
