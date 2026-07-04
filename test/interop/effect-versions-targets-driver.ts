import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunHttpClient from "@effect/platform-bun/BunHttpClient";
import { Duration, Effect, Layer, Option, Schema } from "effect";
import * as NetworkHttp from "../../src/network/Http.ts";
import * as Protocol from "../../src/Protocol.ts";
import * as Resonate from "../../src/Resonate.ts";
import { ResonateContext } from "../../src/ResonateContext.ts";
import * as Worker from "../../src/Worker.ts";

declare const Bun: { readonly env: Record<string, string | undefined> };

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const groupName = Bun.env.RESONATE_GROUP ?? "default";
const pidName = Bun.env.RESONATE_PID ?? "versions-targets-worker";

const group = Protocol.WorkerGroup.make(groupName);
const pid = Protocol.ProcessId.make(pidName);
const VersionOne = Protocol.FunctionVersion.make(1);
const VersionTwo = Protocol.FunctionVersion.make(2);

const VersionsTargetsV1 = Resonate.function({ name: "versionsTargets", payload: Schema.String, version: VersionOne });
const VersionsTargetsV2 = Resonate.function({ name: "versionsTargets", payload: Schema.String, version: VersionTwo });
const App = Resonate.group(VersionsTargetsV1, VersionsTargetsV2);

const versioned = (registered: number) => (input: string) =>
  Effect.gen(function* (): Effect.fn.Return<unknown, never, ResonateContext> {
    const ctx = yield* ResonateContext;
    return { function: "versioned", registered, ctxVersion: ctx.info.version, input };
  });

const networkLayer = NetworkHttp.layer({ url, group, pid }).pipe(Layer.provideMerge(BunHttpClient.layer));

const clientLayer = Resonate.Client.layer({ group, pid, ttl: Duration.seconds(5) }).pipe(
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const workerLayer = Worker.layer({ group: App, worker: { group, pid, ttl: Duration.seconds(5) } }).pipe(
  Layer.provideMerge(Layer.succeed(Resonate.Handler(VersionsTargetsV1), versioned(1))),
  Layer.provideMerge(Layer.succeed(Resonate.Handler(VersionsTargetsV2), versioned(2))),
  Layer.provideMerge(networkLayer),
  Layer.provideMerge(BunCrypto.layer),
);

const program = Effect.gen(function* () {
  const client = yield* Resonate.Client;
  const pollAnycast = Protocol.TargetAddress.pollAny({
    group,
    id: Option.some(pid),
  });
  const pollUnicast = Protocol.TargetAddress.pollUni({ group, id: pid });

  const observed: Record<string, unknown> = {
    defaults: {
      latestOptionVersion: 0,
      latestWireVersion: yield* Schema.encodeUnknownEffect(Protocol.FunctionVersionFromWire)("latest"),
    },
    targets: {
      pollAnycast: pollAnycast.address,
      pollUnicast: pollUnicast.address,
    },
    limitations: {
      localAnycast: "not exercised: Local is an in-memory runtime, not the single HTTP debug server process",
      localUnicast: "not exercised: Local is an in-memory runtime, not the single HTTP debug server process",
      pollUnicast:
        "not invoked by both drivers because the Effect client currently accepts WorkerGroup targets rather than arbitrary target address strings",
    },
  };

  observed.versions = {
    explicitV1: yield* client.rpc({
      targetFunction: VersionsTargetsV1,
      executionId: Protocol.ExecutionId.make("versions-targets-v1"),
      args: ["explicit-v1"],
      options: { target: group, version: VersionOne },
    }),
    explicitV2: yield* client.rpc({
      targetFunction: VersionsTargetsV2,
      executionId: Protocol.ExecutionId.make("versions-targets-v2"),
      args: ["explicit-v2"],
      options: { target: group, version: VersionTwo },
    }),
    latest: yield* client.rpc({
      targetFunction: VersionsTargetsV2,
      executionId: Protocol.ExecutionId.make("versions-targets-latest"),
      args: ["latest"],
      options: { target: group, version: "latest" },
    }),
  };

  observed.targetResults = {
    pollAnycast: yield* client.rpc({
      targetFunction: VersionsTargetsV2,
      executionId: Protocol.ExecutionId.make("versions-targets-poll-any"),
      args: ["poll-anycast"],
      options: { target: group, version: VersionTwo },
    }),
  };

  return observed;
});

const result = await Effect.runPromise(program.pipe(Effect.provide(Layer.mergeAll(clientLayer, workerLayer))));

console.log(JSON.stringify(result));
