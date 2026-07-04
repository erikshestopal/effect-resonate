const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Resonate } = await import(nativeSdk);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "kitchen-sink-worker";

const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });

async function localStep(_ctx, input) {
  return { step: "localStep", input };
}

async function localAsync(_ctx, input) {
  return { step: "localAsync", input };
}

function* remoteChild(_ctx, input) {
  return { step: "remoteChild", input };
}

function* detachedChild(_ctx, input) {
  return { step: "detachedChild", input };
}

function* kitchenSink(ctx, input) {
  const local = yield* ctx.run(localStep, input);
  const pendingLocal = yield* ctx.beginRun(localAsync, input);
  const rpc = yield* ctx.rpc(remoteChild, "rpc", ctx.options({ target: group }));
  const pendingRpc = yield* ctx.beginRpc(remoteChild, "beginRpc", ctx.options({ target: group }));
  const detached = yield* ctx.detached(detachedChild, "detached", ctx.options({ target: group }));
  yield* ctx.sleep(1);
  return {
    local,
    begunLocal: yield* pendingLocal,
    rpc,
    begunRpc: yield* pendingRpc,
    detached: yield* detached,
  };
}

resonate.register(kitchenSink);
resonate.register(localStep);
resonate.register(localAsync);
resonate.register(remoteChild);
resonate.register(detachedChild);

console.log("native-kitchen-sink-worker-ready");
setInterval(() => {}, 1_000);
