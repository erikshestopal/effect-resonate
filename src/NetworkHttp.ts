import { Duration, Effect, Filter, Layer, Option, Schedule, Schema, Stream } from "effect";
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

export const layer = (options: NetworkHttpOptions): Layer.Layer<ResonateNetwork, never, HttpClient.HttpClient> =>
  Layer.effect(
    ResonateNetwork,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const group = Protocol.WorkerGroup.make(options.group ?? "default");
      const pid = Protocol.ProcessId.make(options.pid ?? "local");
      const configuredPid = Option.as(Option.fromNullishOr(options.pid), pid);
      const token = Option.fromNullishOr(options.token);
      const commonHeaders = Option.match(token, {
        onNone: () => ({ "Content-Type": "application/json" }),
        onSome: (token) => ({ "Content-Type": "application/json", Authorization: `Bearer ${token}` }),
      });

      const send = Effect.fn("NetworkHttp.send")(function* <K extends Protocol.RequestKind>(
        request: Protocol.Request<K>,
      ) {
        const wire = yield* encodeRequest(request);
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
        if (!protocolStatuses.has(response.status)) {
          return yield* new TransportError({ reason: "ConnectionLost", cause: response.status });
        }
        const body = yield* response.json.pipe(
          Effect.mapError((cause) => new TransportError({ reason: "MalformedResponse", cause })),
        );
        return yield* decodeResponse(request)(body);
      });

      const connectMessages = Stream.unwrap(
        client
          .get(
            `${options.url.endsWith("/") ? options.url.slice(0, -1) : options.url}${Protocol.TargetAddress.pollUni(group, pid).pollPath}`,
            { headers: commonHeaders, accept: "text/event-stream" },
          )
          .pipe(
            Effect.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })),
            Effect.flatMap((response) =>
              response.status === 401 || response.status === 403
                ? Effect.fail(new TransportError({ reason: "Unauthorized", cause: response.status }))
                : response.status !== 200
                  ? Effect.fail(new TransportError({ reason: "ConnectionLost", cause: response.status }))
                  : Effect.succeed(response),
            ),
            Effect.map((response) =>
              response.stream.pipe(
                Stream.mapError((cause) => new TransportError({ reason: "ConnectionLost", cause })),
                Stream.decodeText(),
                Stream.splitLines,
                Stream.filterMap(
                  Filter.fromPredicateOption((line) =>
                    line.startsWith("data:") ? Option.some(line.slice("data:".length).trimStart()) : Option.none(),
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

      return ResonateNetwork.of({
        send,
        messages: connectMessages.pipe(Stream.retry(retryReconnect)),
        match: (target) => Protocol.TargetAddress.pollAny(target),
        unicast: Protocol.TargetAddress.pollUni(group, pid),
        anycast: (target) => Protocol.TargetAddress.pollAny(target, configuredPid),
      });
    }),
  );
