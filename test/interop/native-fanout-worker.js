const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Resonate } = await import(nativeSdk);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "native-fanout-worker";

const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });

async function sendEmail(_ctx, event) {
  return { channel: "email", destination: event.email, ok: true };
}

async function sendSms(_ctx, event) {
  return { channel: "sms", destination: event.phone, ok: true };
}

async function sendSlack(_ctx, event) {
  return { channel: "slack", destination: event.orderId, ok: true };
}

async function sendPush(_ctx, event) {
  return { channel: "push", destination: event.orderId, ok: true };
}

function* notifyAll(ctx, event) {
  const email = yield* ctx.beginRun(sendEmail, event);
  const sms = yield* ctx.beginRun(sendSms, event);
  const slack = yield* ctx.beginRun(sendSlack, event);
  const push = yield* ctx.beginRun(sendPush, event);
  return [yield* email, yield* sms, yield* slack, yield* push];
}

resonate.register(notifyAll);
resonate.register(sendEmail);
resonate.register(sendSms);
resonate.register(sendSlack);
resonate.register(sendPush);

console.log("native-fanout-worker-ready");
setInterval(() => {}, 1_000);
