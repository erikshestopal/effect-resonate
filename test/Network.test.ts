import { describe, expect, it } from "@effect/vitest";
import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import { Effect, Exit, Option, Schema, SchemaParser, Stream } from "effect";
import { TransportError } from "../src/Errors.ts";
import { checkEnvelope, makeRequestHead, ResonateNetwork } from "../src/Network.ts";
import * as Protocol from "../src/Protocol.ts";
import { TestNetwork } from "../src/testing.ts";

const promiseGet = (head: Protocol.RequestHead) =>
  Protocol.PromiseGetRequest.make({ head, data: { id: Protocol.PromiseId.make("foo.1") } });

const pendingPromiseWire = {
  id: "foo.1",
  state: "pending",
  param: {},
  value: {},
  tags: {},
  timeoutAt: 1750000060000,
  createdAt: 1750000000000,
};

describe("envelope helpers", () => {
  it.effect("assigns fresh corrIds and carries the protocol version", () =>
    Effect.gen(function* () {
      const first = yield* makeRequestHead;
      const second = yield* makeRequestHead;
      expect(first.version).toBe("2026-04-01");
      expect(first.corrId).not.toBe(second.corrId);
    }).pipe(Effect.provide(BunCrypto.layer)),
  );

  it.effect("rejects a response with a mismatched corrId as CorrelationMismatch", () =>
    Effect.gen(function* () {
      const head = yield* makeRequestHead;
      const request = promiseGet(head);
      const response = Protocol.PromiseGetResponse.make({
        kind: "promise.get",
        head: { corrId: Protocol.CorrelationId.make("someone-else"), status: 404, version: "2026-04-01" },
        data: "not found",
      });
      const exit = yield* Effect.exit(checkEnvelope(request)(response));
      expect(Exit.isFailure(exit)).toBe(true);
      const error = yield* Effect.flip(checkEnvelope(request)(response));
      expect(error).toBeInstanceOf(TransportError);
      expect(error.reason).toBe("CorrelationMismatch");
    }).pipe(Effect.provide(BunCrypto.layer)),
  );

  it.effect("maps 401/403 to the terminal Unauthorized transport failure", () =>
    Effect.gen(function* () {
      const head = yield* makeRequestHead;
      const request = promiseGet(head);
      for (const status of [401, 403] as const) {
        const response = Protocol.PromiseGetResponse.make({
          kind: "promise.get",
          head: { corrId: head.corrId, status, version: "2026-04-01" },
          data: "denied",
        });
        const error = yield* Effect.flip(checkEnvelope(request)(response));
        expect(error.reason).toBe("Unauthorized");
      }
    }).pipe(Effect.provide(BunCrypto.layer)),
  );

  it.effect("passes matching responses through untouched, protocol statuses as data", () =>
    Effect.gen(function* () {
      const head = yield* makeRequestHead;
      const request = promiseGet(head);
      const notFound = Protocol.PromiseGetResponse.make({
        kind: "promise.get",
        head: { corrId: head.corrId, status: 404, version: "2026-04-01" },
        data: "not found",
      });
      expect(yield* checkEnvelope(request)(notFound)).toBe(notFound);
    }).pipe(Effect.provide(BunCrypto.layer)),
  );
});

describe("TestNetwork", () => {
  const scripted: (request: Protocol.Request) => Effect.Effect<Protocol.Response, TransportError> = (request) => {
    if (request.kind !== "promise.get") {
      return Effect.fail(new TransportError({ reason: "MalformedResponse", cause: request }));
    }
    return Effect.succeed(
      Protocol.PromiseGetResponse.make({
        kind: "promise.get",
        head: { corrId: request.head.corrId, status: 200, version: "2026-04-01" },
        data: { promise: Schema.decodeUnknownSync(Protocol.PromiseRecordFromWire)(pendingPromiseWire) },
      }),
    );
  };

  it.effect("round-trips a scripted typed exchange through the wire schemas", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      const head = yield* makeRequestHead;
      const response = yield* network.send(promiseGet(head));
      const isSuccess = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);
      expect(isSuccess(response)).toBe(true);
      if (!isSuccess(response)) {
        return;
      }
      expect(response.data.promise.state).toBe("pending");
      const test = yield* TestNetwork;
      const seen = yield* test.requests;
      expect(seen).toHaveLength(1);
    }).pipe(Effect.provide([TestNetwork.layer(scripted), BunCrypto.layer])),
  );

  it.effect("pushed messages arrive on the stream in order", () =>
    Effect.gen(function* () {
      const test = yield* TestNetwork;
      const network = yield* ResonateNetwork;
      const execute = Protocol.ExecuteMessage.make({
        head: {},
        data: { task: { id: Protocol.TaskId.make("foo.1"), version: Protocol.TaskVersion.make(1) } },
      });
      const halt = Protocol.ExecuteMessage.make({
        head: {},
        data: { task: { id: Protocol.TaskId.make("foo.2"), version: Protocol.TaskVersion.make(2) } },
      });
      yield* test.push(execute);
      yield* test.push(halt);
      const received = yield* Stream.take(network.messages, 2).pipe(Stream.runCollect);
      expect(received).toEqual([execute, halt]);
    }).pipe(Effect.provide([TestNetwork.layer(scripted), BunCrypto.layer])),
  );

  it.effect("exposes native-shaped addresses", () =>
    Effect.gen(function* () {
      const network = yield* ResonateNetwork;
      expect(network.unicast.address).toBe("poll://uni@payments/pid-9");
      expect(network.anycast(Protocol.WorkerGroup.make("payments")).address).toBe("poll://any@payments/pid-9");
      expect(network.match(Protocol.WorkerGroup.make("gpu")).address).toBe("poll://any@gpu");
      expect(Option.isNone(network.match(Protocol.WorkerGroup.make("gpu")).id)).toBe(true);
    }).pipe(Effect.provide([TestNetwork.layer(scripted, { group: "payments", pid: "pid-9" }), BunCrypto.layer])),
  );
});
