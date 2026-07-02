import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import * as BunHttpServer from "@effect/platform-bun/BunHttpServer";
import { Duration, Effect, Exit, Fiber, Layer, Option, Ref, Schema, SchemaParser, Stream } from "effect";
import { TestClock } from "effect/testing";
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { TransportError } from "../src/Errors.ts";
import { makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as NetworkHttp from "../src/NetworkHttp.ts";
import * as Protocol from "../src/Protocol.ts";

const isGot = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);

const promiseGet = (head: Protocol.RequestHead) =>
  Protocol.PromiseGetRequest.make({ head, data: { id: Protocol.PromiseId.make("p1") } });

const okPromise = (request: Protocol.Request<"promise.get">) =>
  Protocol.PromiseGetResponse.make({
    kind: "promise.get",
    head: { corrId: request.head.corrId, status: 200, version: "2026-04-01" },
    data: {
      promise: new Protocol.PromisePending({
        id: Protocol.PromiseId.make("p1"),
        state: "pending",
        param: {},
        value: {},
        tags: Protocol.emptyTags,
        timeoutAt: Schema.decodeUnknownSync(Protocol.Timestamp)(1_000),
        createdAt: Schema.decodeUnknownSync(Protocol.Timestamp)(0),
      }),
    },
  });

const responseJson = (response: Protocol.Response, status = response.head.status) =>
  HttpServerResponse.schemaJson(Protocol.ResponseFromWire)(response, {
    status,
    headers: { "Content-Type": "application/json" },
  });

const networkLayer = (url: string, options?: Omit<NetworkHttp.NetworkHttpOptions, "url">) =>
  NetworkHttp.layer({ url, ...options }).pipe(Layer.provide(BunHttpClient.layer));

const serverUrl = HttpServer.addressFormattedWith((url) => Effect.succeed(url));

const serverLayer = Layer.mergeAll(BunHttpServer.layer({ port: 0 }), BunCrypto.layer);

describe("NetworkHttp.send", () => {
  it.effect("POSTs the envelope to the single endpoint and decodes protocol statuses", () =>
    Effect.gen(function* () {
      const seen = yield* Ref.make<ReadonlyArray<Protocol.Request<"promise.get">>>([]);
      yield* HttpRouter.serve(
        HttpRouter.add(
          "POST",
          "/",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.schemaBodyJson(Protocol.PromiseGetRequest);
            yield* Ref.update(seen, (requests) => [...requests, request]);
            return yield* responseJson(okPromise(request));
          }),
        ),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);

      const url = yield* serverUrl;
      const network = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(url)));
      const head = yield* makeRequestHead;
      const response = yield* network.send(promiseGet(head));
      expect(isGot(response)).toBe(true);
      if (!isGot(response)) {
        return;
      }
      expect(response.data.promise.id).toBe("p1");
      const requests = yield* Ref.get(seen);
      expect(requests).toHaveLength(1);
      expect(requests[0]?.head.corrId).toBe(head.corrId);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("normalizes 500, garbage JSON, and mismatched corrId to transport failures", () =>
    Effect.gen(function* () {
      const head = yield* makeRequestHead;
      const request = promiseGet(head);

      yield* HttpRouter.serve(
        HttpRouter.add("POST", "/server-error", HttpServerResponse.text("server error", { status: 500 })),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const baseUrl = yield* serverUrl;
      const network500 = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(`${baseUrl}/server-error`)));
      expect((yield* Effect.flip(network500.send(request))).reason).toBe("ConnectionLost");

      yield* HttpRouter.serve(
        HttpRouter.add("POST", "/garbage", HttpServerResponse.text("not json", { status: 200 })),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const networkGarbage = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(`${baseUrl}/garbage`)));
      expect((yield* Effect.flip(networkGarbage.send(request))).reason).toBe("MalformedResponse");

      yield* HttpRouter.serve(
        HttpRouter.add(
          "POST",
          "/mismatch",
          responseJson(
            Protocol.PromiseGetResponse.make({
              kind: "promise.get",
              head: { corrId: Protocol.CorrelationId.make("other"), status: 404, version: "2026-04-01" },
              data: "missing",
            }),
            404,
          ),
        ),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const networkMismatch = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(`${baseUrl}/mismatch`)));
      expect((yield* Effect.flip(networkMismatch.send(request))).reason).toBe("CorrelationMismatch");
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("maps 401 to terminal Unauthorized", () =>
    Effect.gen(function* () {
      yield* HttpRouter.serve(HttpRouter.add("POST", "/", HttpServerResponse.text("denied", { status: 401 })), {
        disableLogger: true,
        disableListenLog: true,
      }).pipe(Layer.build);
      const network = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(yield* serverUrl, { token: "secret" })));
      const head = yield* makeRequestHead;
      const error = yield* Effect.flip(network.send(promiseGet(head)));
      expect(error).toBeInstanceOf(TransportError);
      expect(error.reason).toBe("Unauthorized");
    }).pipe(Effect.provide(serverLayer)),
  );
});

describe("NetworkHttp.messages", () => {
  it.effect("decodes SSE data frames to typed messages and exposes poll addresses", () =>
    Effect.gen(function* () {
      const execute = Protocol.ExecuteMessage.make({
        head: {},
        data: { task: { id: Protocol.TaskId.make("p1"), version: Protocol.TaskVersion.make(1) } },
      });
      const seen = yield* Ref.make<ReadonlyArray<string>>([]);
      yield* HttpRouter.serve(
        HttpRouter.add("GET", "/poll/workers/pid-1", (request) =>
          Effect.gen(function* () {
            yield* Ref.update(seen, (urls) => [...urls, request.url]);
            return HttpServerResponse.text(
              `data: ${JSON.stringify(Schema.encodeUnknownSync(Protocol.Message)(execute))}\n\n`,
              { contentType: "text/event-stream" },
            );
          }),
        ),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const network = yield* ResonateNetwork.pipe(
        Effect.provide(networkLayer(yield* serverUrl, { group: "workers", pid: "pid-1" })),
      );

      expect(network.unicast.address).toBe("poll://uni@workers/pid-1");
      expect(network.anycast(Protocol.WorkerGroup.make("workers")).address).toBe("poll://any@workers/pid-1");
      expect(network.match(Protocol.WorkerGroup.make("gpu")).address).toBe("poll://any@gpu");
      expect(Option.isNone(network.match(Protocol.WorkerGroup.make("gpu")).id)).toBe(true);
      const [message] = yield* Stream.runCollect(Stream.take(network.messages, 1));
      expect(message).toEqual(execute);
      expect(yield* Ref.get(seen)).toEqual(["/poll/workers/pid-1"]);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("fails terminally on poll auth errors", () =>
    Effect.gen(function* () {
      yield* HttpRouter.serve(
        HttpRouter.add("GET", "/poll/default/local", HttpServerResponse.text("denied", { status: 401 })),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const network = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(yield* serverUrl)));
      const exit = yield* Stream.runCollect(Stream.take(network.messages, 1)).pipe(Effect.exit);
      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("reconnects poll streams with exponential backoff after connection loss", () =>
    Effect.gen(function* () {
      const execute = Protocol.ExecuteMessage.make({
        head: {},
        data: { task: { id: Protocol.TaskId.make("p1"), version: Protocol.TaskVersion.make(1) } },
      });
      const attempts = yield* Ref.make(0);
      yield* HttpRouter.serve(
        HttpRouter.add("GET", "/poll/default/local", () =>
          Effect.gen(function* () {
            const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1);
            if (attempt === 1) {
              return HttpServerResponse.text("dropped", { status: 500 });
            }
            return HttpServerResponse.text(
              `data: ${JSON.stringify(Schema.encodeUnknownSync(Protocol.Message)(execute))}\n\n`,
              { contentType: "text/event-stream" },
            );
          }),
        ),
        { disableLogger: true, disableListenLog: true },
      ).pipe(Layer.build);
      const network = yield* ResonateNetwork.pipe(Effect.provide(networkLayer(yield* serverUrl)));
      const fiber = yield* Stream.runCollect(Stream.take(network.messages, 1)).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* TestClock.adjust(Duration.millis(999));
      expect(yield* Ref.get(attempts)).toBe(1);
      yield* TestClock.adjust(Duration.millis(1001));
      yield* Effect.yieldNow;
      const [message] = yield* Fiber.join(fiber);
      expect(message).toEqual(execute);
      expect(yield* Ref.get(attempts)).toBe(2);
    }).pipe(Effect.provide(serverLayer)),
  );
});
