const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Resonate } = await import(nativeSdk);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "native-worker";

const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });

function* NativeEcho(_ctx, value) {
  return `native:${value}`;
}

function* NativeCallsEffect(ctx, value) {
  return yield* ctx.rpc("EffectEcho", value, ctx.options({ target: group }));
}

function* NativeAwaitsExternal(ctx) {
  const promise = yield* ctx.promise();
  return yield* promise;
}

resonate.register(NativeEcho);
resonate.register(NativeCallsEffect);
resonate.register(NativeAwaitsExternal);

console.log("native-worker-ready");
setInterval(() => {}, 1_000);
