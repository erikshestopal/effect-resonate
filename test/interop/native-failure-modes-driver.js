const nativeSdk = `../../repos/${"resonate-sdk-ts"}/src/index.ts`;
const nativeRetries = `../../repos/${"resonate-sdk-ts"}/src/retries.ts`;
const { Codec, Resonate } = await import(nativeSdk);
const { Constant, Exponential, Linear, Never } = await import(nativeRetries);

const url = Bun.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Bun.env.RESONATE_GROUP ?? "default";
const pid = Bun.env.RESONATE_PID ?? "failure-modes-worker";
const codec = new Codec();
const resonate = new Resonate({ url, group, pid, ttl: 5_000, logLevel: "error" });
const observed = {};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const recordExit = async (effect) => {
  try {
    return { status: "success", value: await effect() };
  } catch (error) {
    return { status: "failure", name: error?.name, message: String(error?.message ?? error) };
  }
};

class NonRetryableFailure extends Error {}
const stableError = (name, message) => {
  const error = new Error(message);
  error.name = name;
  error.stack = `${name}: ${message}\n    at parity`;
  return error;
};
const stableAggregateError = () => {
  const error = new AggregateError([stableError("Error", "one")], "many");
  error.stack = "AggregateError: many\n    at parity";
  return error;
};

function* pendingExternal(ctx, label) {
  const approval = yield* ctx.promise({ tags: { label } });
  return yield* approval;
}
function* attachedParent(ctx, label) {
  const child = yield* ctx.beginRpc(pendingExternal, `${label}-child`, ctx.options({ target: group }));
  return yield* child;
}
function* detachedParent(ctx, label) {
  const child = yield* ctx.detached(pendingExternal, `${label}-detached`, ctx.options({ target: group }));
  return { detached: child.id };
}
function* retrying(ctx, spec) {
  observed.retryAttempts ??= {};
  observed.retryAttempts[spec.label] ??= [];
  observed.retryAttempts[spec.label].push(ctx.info.attempt);
  if (spec.kind === "nonRetryable") {
    throw stableError("Error", "non-retryable");
  }
  if (spec.kind === "error") {
    throw spec.error;
  }
  return { attempt: ctx.info.attempt };
}
function* encodingFailure(_ctx, value) {
  throw value;
}
function* encodingValue(_ctx, value) {
  return value;
}

resonate.register(pendingExternal);
resonate.register(attachedParent);
resonate.register(detachedParent);
resonate.register(retrying);
resonate.register(encodingFailure);
resonate.register(encodingValue);

const root = await resonate.beginRpc(
  "failure-cancel-root",
  "pendingExternal",
  "root",
  resonate.options({ target: group }),
);
await sleep(250);
await resonate.promises.cancel(root.id);
await recordExit(() => root.result());

const attached = await resonate.beginRpc(
  "failure-cancel-attached",
  "attachedParent",
  "attached",
  resonate.options({ target: group }),
);
await sleep(500);
await resonate.promises.cancel("failure-cancel-attached.0");
await recordExit(() => attached.result());

const detached = await resonate.rpc(
  "failure-cancel-detached",
  "detachedParent",
  "detached",
  resonate.options({ target: group }),
);
await resonate.promises.cancel(detached.detached);
observed.cancelDetachedChild = { detached };

const external = await resonate.beginRpc(
  "failure-cancel-external",
  "pendingExternal",
  "external",
  resonate.options({ target: group }),
);
await sleep(250);
await resonate.promises.cancel("failure-cancel-external.0");
await recordExit(() => external.result());

const policies = {
  constant: new Constant({ delay: 0, maxRetries: 2 }),
  linear: new Linear({ delay: 0, maxRetries: 2 }),
  exponential: new Exponential({ delay: 0, factor: 2, maxRetries: 2, maxDelay: 1 }),
  never: new Never(),
  exhausted: new Constant({ delay: 0, maxRetries: 1 }),
  timeoutExceeded: new Constant({ delay: 1_000, maxRetries: 3 }),
};
const retries = {};
for (const [label, retryPolicy] of Object.entries(policies)) {
  retries[label] = await recordExit(() =>
    resonate.rpc(
      `failure-retry-${label}`,
      "retrying",
      { label, kind: "error", error: `${label}-boom` },
      resonate.options({ target: group, retryPolicy, timeout: label === "timeoutExceeded" ? 250 : 5_000 }),
    ),
  );
}
retries.nonRetryable = await recordExit(() =>
  resonate.rpc(
    "failure-retry-non-retryable",
    "retrying",
    { label: "nonRetryable", kind: "nonRetryable" },
    resonate.options({
      target: group,
      retryPolicy: new Constant({ delay: 0, maxRetries: 3 }),
      nonRetryableErrors: [NonRetryableFailure],
    }),
  ),
);

const encodedValues = [
  "string-error",
  { object: true },
  stableError("Error", "native-error"),
  stableAggregateError(),
  undefined,
  null,
  Infinity,
  new Date(0),
  [1, undefined, null],
  { nested: { array: [1, { two: 2 }] } },
];
let encodingCount = 0;
for (let index = 0; index < encodedValues.length; index += 1) {
  const value = encodedValues[index];
  await recordExit(() =>
    resonate.rpc(`failure-encoding-error-${index}`, "encodingFailure", value, resonate.options({ target: group })),
  );
  encodingCount += 1;
}
observed.encodedValue = await resonate.rpc(
  "failure-encoding-value",
  "encodingValue",
  { undefined, nil: null, infinity: Infinity, date: new Date(0), array: [undefined, null], nested: { ok: true } },
  resonate.options({ target: group }),
);
observed.retryResultStatuses = Object.fromEntries(
  Object.entries(retries).map(([label, result]) => [label, result.status]),
);
observed.encodingCount = encodingCount;
observed.typedExternalSchemaHeaders = {
  resolve: { ...codec.encode("ok"), headers: { "resonate:schema": "nativeTyped" } },
};

await resonate.stop();
console.log(JSON.stringify(observed));
