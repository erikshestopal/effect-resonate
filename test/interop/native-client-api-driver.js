const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const nativeRetries = `../../repos/${"resonate-sdk-ts"}/src/retries.ts`;
const { Codec, Resonate } = await import(nativeSdk);
const { Constant } = await import(nativeRetries);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "client-api-worker";

const codec = new Codec();
const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });

const schemaHeader = { "resonate:schema": "approval" };
const approvalValue = (value) => ({ ...codec.encode(value), headers: schemaHeader });

const observed = {};

function* clientEcho(_ctx, input) {
  return { step: "clientEcho", input };
}

function* retryOnce(ctx, input) {
  if (ctx.info.attempt === 0) {
    throw "retry-once";
  }
  return { step: "retryOnce", input, attempt: ctx.info.attempt };
}

function* awaitApproval(ctx, input) {
  const approval = yield* ctx.promise();
  return { input, approval: yield* approval };
}

function* alwaysReject(_ctx, input) {
  throw { step: "alwaysReject", input };
}

resonate.register(clientEcho);
resonate.register(retryOnce);
resonate.register(awaitApproval);
resonate.register(alwaysReject);

observed.run = await resonate.run("client-run", "clientEcho", "run");

const beginRun = await resonate.beginRun("client-begin-run", "clientEcho", "beginRun");
observed.beginRun = await beginRun.result();
observed.get = await (await resonate.get("client-begin-run")).result();

observed.rpc = await resonate.rpc("client-rpc", "clientEcho", "rpc", resonate.options({ target: group }));

const beginRpc = await resonate.beginRpc(
  "client-begin-rpc",
  "clientEcho",
  "beginRpc",
  resonate.options({ target: group }),
);
observed.beginRpc = await beginRpc.result();

const resolving = await resonate.beginRpc(
  "client-resolve",
  "awaitApproval",
  "resolve",
  resonate.options({ target: group }),
);
await new Promise((resolve) => setTimeout(resolve, 500));
await resonate.promises.resolve("client-resolve.0", approvalValue("approved"));
observed.resolve = await resolving.result();

const rejecting = await resonate.beginRpc(
  "client-reject",
  "awaitApproval",
  "reject",
  resonate.options({ target: group }),
);
await new Promise((resolve) => setTimeout(resolve, 500));
await resonate.promises.reject("client-reject.0", approvalValue("denied"));
try {
  await rejecting.result();
} catch {
  observed.reject = "rejected";
}

await resonate.beginRpc("client-cancel", "awaitApproval", "cancel", resonate.options({ target: group }));
await new Promise((resolve) => setTimeout(resolve, 500));
await resonate.promises.cancel("client-cancel");
observed.cancel = "canceled";

observed.retry = await resonate.rpc(
  "client-retry",
  "retryOnce",
  "retry",
  resonate.options({ target: group, retryPolicy: new Constant({ delay: 0, maxRetries: 1 }) }),
);

try {
  await resonate.rpc("client-error", "alwaysReject", "error", resonate.options({ target: group }));
} catch {
  observed.error = "errored";
}

const schedule = await resonate.schedule(
  "client-schedule",
  "* * * * *",
  "clientEcho",
  "schedule",
  resonate.options({ target: group }),
);
const scheduleRecord = await resonate.schedules.get("client-schedule");
observed.schedule = {
  id: scheduleRecord.id,
  cron: scheduleRecord.cron,
  promiseId: scheduleRecord.promiseId,
  promiseTimeout: scheduleRecord.promiseTimeout,
  promiseParam: codec.decode(scheduleRecord.promiseParam),
  promiseTags: scheduleRecord.promiseTags,
};
await schedule.delete();

await resonate.stop();

console.log(JSON.stringify(observed));
