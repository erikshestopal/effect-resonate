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

function* clientEcho(_ctx, input) {
  return { step: "clientEcho", input };
}

function* awaitApproval(ctx, input) {
  const approval = yield* ctx.promise();
  return { input, approval: yield* approval };
}

function* retryOnce(ctx, input) {
  if (ctx.info.attempt === 0) {
    throw "retry-once";
  }
  return { step: "retryOnce", input, attempt: ctx.info.attempt };
}

function* alwaysReject(_ctx, input) {
  throw { step: "alwaysReject", input };
}

function* kitchenSink(ctx, input) {
  const local = yield* ctx.run(localStep, input);
  const pendingLocal = yield* ctx.beginRun(localAsync, input);
  const rpc = yield* ctx.rpc(remoteChild, "rpc", ctx.options({ target: group }));
  const pendingRpc = yield* ctx.beginRpc(remoteChild, "beginRpc", ctx.options({ target: group }));
  const detached = yield* ctx.detached(detachedChild, "detached", ctx.options({ target: group }));
  yield* ctx.sleep(1);
  yield* ctx.sleep({ until: new Date(Date.now() + 1) });
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
resonate.register(clientEcho);
resonate.register(awaitApproval);
resonate.register(retryOnce);
resonate.register(alwaysReject);

console.log("native-kitchen-sink-worker-ready");
setInterval(() => {}, 1_000);
