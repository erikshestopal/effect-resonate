const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Resonate } = await import(nativeSdk);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "versions-targets-worker";

const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });

const observed = {
  defaults: {
    latestOptionVersion: resonate.options().version,
    latestWireVersion: 0,
  },
  targets: {
    pollAnycast: `poll://any@${group}/${pid}`,
    pollUnicast: `poll://uni@${group}/${pid}`,
  },
  limitations: {
    localAnycast: "not exercised: Local is an in-memory runtime, not the single HTTP debug server process",
    localUnicast: "not exercised: Local is an in-memory runtime, not the single HTTP debug server process",
    pollUnicast:
      "not invoked by both drivers because the Effect client currently accepts WorkerGroup targets rather than arbitrary target address strings",
  },
};

function* versionedV1(ctx, input) {
  return { function: "versioned", registered: 1, ctxVersion: ctx.info.version, input };
}

function* versionedV2(ctx, input) {
  return { function: "versioned", registered: 2, ctxVersion: ctx.info.version, input };
}

resonate.register("versionsTargets", versionedV1, { version: 1 });
resonate.register("versionsTargets", versionedV2, { version: 2 });

observed.versions = {
  explicitV1: await resonate.rpc(
    "versions-targets-v1",
    "versionsTargets",
    "explicit-v1",
    resonate.options({ target: group, version: 1 }),
  ),
  explicitV2: await resonate.rpc(
    "versions-targets-v2",
    "versionsTargets",
    "explicit-v2",
    resonate.options({ target: group, version: 2 }),
  ),
  latest: await resonate.rpc(
    "versions-targets-latest",
    "versionsTargets",
    "latest",
    resonate.options({ target: group }),
  ),
};

observed.targetResults = {
  pollAnycast: await resonate.rpc(
    "versions-targets-poll-any",
    "versionsTargets",
    "poll-anycast",
    resonate.options({ target: group, version: 2 }),
  ),
};

await resonate.stop();

console.log(JSON.stringify(observed));
