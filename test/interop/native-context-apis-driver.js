const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Codec, Resonate } = await import(nativeSdk);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? Bun.env.GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? Bun.env.PID ?? "context-apis-native";

const codec = new Codec();
const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });
const approvalValue = (value) => ({ ...codec.encode(value), headers: { "resonate:schema": "approval" } });
const request = async (kind, data = {}) =>
  (
    await (
      await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kind, head: { corrId: `${kind}-${crypto.randomUUID()}`, version: "2026-04-01" }, data }),
      })
    ).json()
  ).data;
const sleep = (millis) => new Promise((resolve) => setTimeout(resolve, millis));
const waitForExternalResumePoint = async (id) => {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const snap = await request("debug.snap");
    const rootSuspended = snap.tasks.some((task) => task.id === id && task.state === "suspended");
    const awaitsExternal = snap.callbacks.some((callback) => callback.awaiter === id && callback.awaited === `${id}.0`);
    if (rootSuspended && awaitsExternal) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`${id} did not reach external resume point`);
};

async function localStep(_ctx, input) {
  return { step: "localStep", input };
}

async function localSlow(_ctx, input) {
  await new Promise((resolve) => setTimeout(resolve, 30));
  return { step: "localSlow", input };
}

async function localFast(_ctx, input) {
  await new Promise((resolve) => setTimeout(resolve, 1));
  return { step: "localFast", input };
}

function* remoteChild(ctx, input) {
  return { step: "remoteChild", input, attempt: ctx.info.attempt };
}

function* detachedChild(ctx, input) {
  return { step: "detachedChild", input, parentId: ctx.parentId };
}

function* contextApis(ctx, input) {
  const external = yield* ctx.promise({ tags: { kind: "approval" } });
  const local = yield* ctx.run(localStep, `${input}:run`);
  const slow = yield* ctx.beginRun(localSlow, `${input}:slow`);
  const fast = yield* ctx.beginRun(localFast, `${input}:fast`);
  const rpc = yield* ctx.rpc(remoteChild, `${input}:rpc`, ctx.options({ target: group }));
  const pendingRpc = yield* ctx.beginRpc(remoteChild, `${input}:beginRpc`, ctx.options({ target: group }));
  const detached = yield* ctx.detached(detachedChild, `${input}:detached`, ctx.options({ target: group }));

  yield* ctx.sleep({ for: 1 });
  yield* ctx.sleep({ until: new Date(Date.now() + 1) });

  return {
    input,
    local,
    fast: yield* fast,
    approval: yield* external,
    slow: yield* slow,
    rpc,
    beginRpc: yield* pendingRpc,
    detached: yield* detached,
  };
}

resonate.register(contextApis);
resonate.register(localStep);
resonate.register(localSlow);
resonate.register(localFast);
resonate.register(remoteChild);
resonate.register(detachedChild);

const run = await resonate.beginRpc("context-apis", "contextApis", "driver", resonate.options({ target: group }));
await waitForExternalResumePoint("context-apis");
await resonate.promises.resolve("context-apis.0", approvalValue("approved-by-driver"));
const observed = await run.result();
await resonate.stop();

console.log(JSON.stringify({ scenario: "context-apis", observed }));
