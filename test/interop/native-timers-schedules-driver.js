const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const { Codec, Resonate } = await import(nativeSdk);
const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "timers-schedules-worker";
const codec = new Codec();
const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });
const observed = {};
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
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const waitForPromiseState = async (id, state) => {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const snap = await request("debug.snap");
    if (snap.promises.some((promise) => promise.id === id && promise.state === state)) {
      return;
    }
    await sleep(100);
  }
  throw new Error(`${id} did not reach ${state}`);
};
function* sleepFor(ctx, ms) {
  yield* ctx.sleep({ for: ms });
  return { slept: ms };
}
function* sleepUntil(ctx, time) {
  yield* ctx.sleep({ until: new Date(time) });
  return { until: time };
}
function* childSleeper(ctx, ms) {
  return yield* ctx.rpc(sleepFor, ms, ctx.options({ target: group }));
}
function* externalTimeout(ctx, ms) {
  const p = yield* ctx.promise({ timeout: ms });
  return yield* p;
}
function* scheduledEcho(_ctx, input) {
  return { scheduled: input };
}
resonate.register(sleepFor);
resonate.register(sleepUntil);
resonate.register(childSleeper);
resonate.register(externalTimeout);
resonate.register(scheduledEcho);
const now = 1_783_141_580_000;
await resonate.beginRpc("timer-root-timeout", "sleepFor", 60_000, resonate.options({ target: group, timeout: 500 }));
await resonate.beginRpc(
  "timer-child-timeout",
  "childSleeper",
  60_000,
  resonate.options({ target: group, timeout: 500 }),
);
await resonate.beginRpc(
  "timer-external-timeout",
  "externalTimeout",
  500,
  resonate.options({ target: group, timeout: 5_000 }),
);
await resonate.beginRpc("timer-sleep-for", "sleepFor", 1_000, resonate.options({ target: group }));
await resonate.beginRpc("timer-sleep-until", "sleepUntil", now + 1_000, resonate.options({ target: group }));
await sleep(500);
observed.tick = await request("debug.tick", { time: now + 2_000 });
await sleep(500);
const schedule = await resonate.schedule(
  "timer-schedule",
  "* * * * *",
  "scheduledEcho",
  "tick",
  resonate.options({ target: group }),
);
observed.scheduleCreated = await resonate.schedules.get("timer-schedule");
observed.scheduleParam = codec.decode(observed.scheduleCreated.promiseParam);
observed.scheduleTick = await request("debug.tick", { time: now + 65_000 });
await sleep(500);
await schedule.delete();
observed.scheduleDeleted = true;
observed.afterDeleteTick = await request("debug.tick", { time: now + 125_000 });
await waitForPromiseState("timer-sleep-for", "resolved");
await resonate.stop();
console.log(JSON.stringify(observed));
