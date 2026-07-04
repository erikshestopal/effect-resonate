import { describe, expect, it } from "@effect/vitest";
import { Effect, Predicate, Schema, SchemaParser } from "effect";
import { decodeResponse, encodeRequest } from "../src/network/network.ts";
import * as Protocol from "../src/Protocol.ts";

const serverPort = 8013;
const serverUrl = `http://127.0.0.1:${serverPort}`;
const group = "debug-snapshot-parity";

const isPromiseGetSuccess = SchemaParser.is(Protocol.PromiseGetSuccessResponse);
const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapSuccessResponse);
const isDebugResetSuccess = SchemaParser.is(Protocol.DebugResetResponse.members[0]);
const timestampKeys = new Set(["createdAt", "settledAt", "timeoutAt", "timeout", "nextRunAt", "lastRunAt"]);

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const kill = (process: Bun.Subprocess) => {
  process.kill();
};

const spawn = (command: ReadonlyArray<string>, env: Record<string, string> = {}) =>
  Bun.spawn([...command], {
    env: { ...Bun.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });

const run = async (command: ReadonlyArray<string>, env: Record<string, string> = {}) => {
  const process = Bun.spawn([...command], { env: { ...Bun.env, ...env }, stdout: "pipe", stderr: "pipe" });
  const [exitCode, stdout, stderr] = await Promise.all([
    process.exited,
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
  ]);
  expect({ command, exitCode, stderr }).toMatchObject({ exitCode: 0 });
  return stdout;
};

const request = async <K extends Protocol.RequestKind>(input: Protocol.Request<K>): Promise<Protocol.Response<K>> => {
  const response = await fetch(serverUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(await Effect.runPromise(encodeRequest(input))),
  });
  return Effect.runPromise(decodeResponse(input)(await response.json()));
};

const requestHead = (corrId: string): Protocol.RequestHead =>
  Protocol.RequestHead.make({ corrId: Protocol.CorrelationId.make(corrId), version: Protocol.protocolVersion });

const getPromise = async (id: Protocol.PromiseId) => {
  const response = await request(
    Protocol.PromiseGetRequest.make({
      kind: "promise.get",
      head: requestHead(`parity-get-${id}-${Date.now()}`),
      data: { id },
    }),
  );
  if (!isPromiseGetSuccess(response)) {
    throw new Error(`promise ${id} was not found`);
  }
  return response.data.promise;
};

const waitForSettled = async (id: Protocol.PromiseId) => {
  for (let attempt = 0; attempt < 80; attempt = attempt + 1) {
    const promise = await getPromise(id);
    if (promise.state !== "pending") {
      return promise;
    }
    await sleep(250);
  }
  throw new Error(`promise ${id} did not settle`);
};

const stripTimestamps = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(stripTimestamps);
  }
  if (Predicate.isObject(value)) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, timestampKeys.has(key) ? "<timestamp>" : stripTimestamps(item)]),
    );
  }
  return value;
};

const debugSnap = async (): Promise<unknown> => {
  const response = await request(
    Protocol.DebugSnapRequest.make({ kind: "debug.snap", head: requestHead(`snap-${Date.now()}`), data: {} }),
  );
  if (!isDebugSnapSuccess(response)) {
    throw new Error(`debug.snap failed: ${JSON.stringify(response.data)}`);
  }
  return stripTimestamps(Schema.encodeSync(Protocol.DebugSnapSuccessResponse)(response).data);
};

const debugReset = async () => {
  const response = await request(
    Protocol.DebugResetRequest.make({ kind: "debug.reset", head: requestHead(`reset-${Date.now()}`), data: {} }),
  );
  if (!isDebugResetSuccess(response)) {
    throw new Error(`debug.reset failed: ${JSON.stringify(response.data)}`);
  }
};

interface ParityCase {
  readonly name: string;
  readonly nativeCommand: ReadonlyArray<string>;
  readonly effectCommand: ReadonlyArray<string>;
  readonly pid: string;
  readonly nativeRoot: Protocol.PromiseId;
  readonly effectRoot: Protocol.PromiseId;
  readonly func: string;
  readonly args: ReadonlyArray<unknown>;
}

const cases: ReadonlyArray<ParityCase> = [
  {
    name: "fanout",
    nativeCommand: ["bun", "test/interop/native-fanout-worker.js"],
    effectCommand: ["bun", "test/interop/effect-fanout-worker.ts"],
    pid: "fanout-worker",
    nativeRoot: Protocol.PromiseId.make("fanout"),
    effectRoot: Protocol.PromiseId.make("fanout"),
    func: "notifyAll",
    args: [{ orderId: "order-1", email: "ada@example.com", phone: "+15550100" }],
  },
  {
    name: "kitchen-sink",
    nativeCommand: ["bun", "test/interop/native-kitchen-sink-worker.js"],
    effectCommand: ["bun", "test/interop/effect-kitchen-sink-worker.ts"],
    pid: "kitchen-sink-worker",
    nativeRoot: Protocol.PromiseId.make("kitchen-sink"),
    effectRoot: Protocol.PromiseId.make("kitchen-sink"),
    func: "kitchenSink",
    args: ["input"],
  },
];

const invoke = (options: {
  readonly id: Protocol.PromiseId;
  readonly func: string;
  readonly args: ReadonlyArray<unknown>;
}) =>
  run([
    "resonate",
    "invoke",
    "--server",
    serverUrl,
    "--func",
    options.func,
    "--target",
    `poll://any@${group}`,
    "--json-args",
    JSON.stringify(options.args),
    options.id,
  ]);

const runWorkerCase = async (options: {
  readonly name: string;
  readonly command: ReadonlyArray<string>;
  readonly func: string;
  readonly args: ReadonlyArray<unknown>;
  readonly pid: string;
  readonly id: Protocol.PromiseId;
}) => {
  const worker = spawn(options.command, {
    RESONATE_URL: serverUrl,
    RESONATE_GROUP: group,
    RESONATE_PID: options.pid,
  });
  const stdout = new Response(worker.stdout).text();
  const stderr = new Response(worker.stderr).text();
  try {
    await sleep(1_000);
    await invoke({ id: options.id, func: options.func, args: options.args });
    const promise = await waitForSettled(options.id);
    expect(promise.state).toBe("resolved");
    return await debugSnap();
  } catch (cause) {
    kill(worker);
    throw new Error(`${options.name} worker failed\nstdout:\n${await stdout}\nstderr:\n${await stderr}`, { cause });
  } finally {
    kill(worker);
  }
};

const runParityCase = async (entry: ParityCase) => {
  const native = await runWorkerCase({
    name: `${entry.name} native`,
    command: entry.nativeCommand,
    func: entry.func,
    args: entry.args,
    pid: entry.pid,
    id: entry.nativeRoot,
  });
  await debugReset();
  const effect = await runWorkerCase({
    name: `${entry.name} effect`,
    command: entry.effectCommand,
    func: entry.func,
    args: entry.args,
    pid: entry.pid,
    id: entry.effectRoot,
  });

  expect(effect).toEqual(native);
  expect(native).toMatchSnapshot(entry.name);
};

const parseJsonOutput = (stdout: string): unknown => {
  const lines = stdout.trim().split("\n");
  const line = lines.at(-1);
  if (Predicate.isUndefined(line)) {
    throw new Error("driver produced no JSON output");
  }
  return JSON.parse(line);
};

const runClientApiSide = async (options: {
  readonly name: string;
  readonly workerCommand?: ReadonlyArray<string>;
  readonly driverCommand: ReadonlyArray<string>;
  readonly workerPid?: string;
  readonly driverPid?: string;
}) => {
  const workerPid = options.workerPid ?? "client-api-worker";
  const driverPid = options.driverPid ?? workerPid;
  const worker = Predicate.isUndefined(options.workerCommand)
    ? undefined
    : spawn(options.workerCommand, {
        RESONATE_URL: serverUrl,
        RESONATE_GROUP: group,
        RESONATE_PID: workerPid,
      });
  const stdout = Predicate.isUndefined(worker) ? Promise.resolve("") : new Response(worker.stdout).text();
  const stderr = Predicate.isUndefined(worker) ? Promise.resolve("") : new Response(worker.stderr).text();
  try {
    await sleep(1_000);
    const observed = parseJsonOutput(
      await run(options.driverCommand, {
        RESONATE_URL: serverUrl,
        RESONATE_GROUP: group,
        RESONATE_PID: driverPid,
      }),
    );
    return {
      observed,
      snapshot: await debugSnap(),
    };
  } catch (cause) {
    if (Predicate.isNotUndefined(worker)) {
      kill(worker);
    }
    throw new Error(`${options.name} client API side failed\nstdout:\n${await stdout}\nstderr:\n${await stderr}`, {
      cause,
    });
  } finally {
    if (Predicate.isNotUndefined(worker)) {
      kill(worker);
    }
  }
};

const runClientApiParity = async () => {
  const native = await runClientApiSide({
    name: "native",
    driverCommand: ["bun", "test/interop/native-client-api-driver.js"],
  });
  await debugReset();
  const effect = await runClientApiSide({
    name: "effect",
    driverCommand: ["bun", "test/interop/effect-client-api-driver.ts"],
  });

  expect(effect.observed).toEqual(native.observed);
  expect(effect.snapshot).toEqual(native.snapshot);
  expect(native).toMatchSnapshot("client-apis");
};

describe("debug snapshot parity", () => {
  it("matches the native TypeScript SDK fanout graph against a debug server", async () => {
    if (Bun.which("resonate") === null) {
      console.error("[DEBUG SNAPSHOT PARITY SKIPPED] resonate CLI not found; install it to run parity snapshots.");
      expect(Bun.which("resonate")).toBeNull();
      return;
    }

    const server = spawn([
      "resonate",
      "dev",
      "--debug",
      "--server-port",
      String(serverPort),
      "--observability-metrics-port",
      "0",
    ]);
    try {
      await sleep(2_000);
      for (const entry of cases) {
        await runParityCase(entry);
        await debugReset();
      }
      await runClientApiParity();
    } finally {
      kill(server);
    }
  }, 120_000);
});
