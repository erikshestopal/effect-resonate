import { describe, expect, it } from "@effect/vitest";
import { Effect, Predicate, Schema, SchemaParser } from "effect";
import { decodeResponse, encodeRequest } from "../src/network/Network.ts";
import * as Protocol from "../src/Protocol.ts";
import { commandExists, spawn, streamText, type Subprocess } from "./support/process.ts";

const serverPort = 20_000 + Math.floor(Math.random() * 20_000);
const serverUrl = `http://127.0.0.1:${serverPort}`;
const group = "debug-snapshot-parity";

const isPromiseGetSuccess = SchemaParser.is(Protocol.PromiseGetSuccessResponse);
const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapSuccessResponse);
const isDebugResetSuccess = SchemaParser.is(Protocol.DebugResetResponse.members[0]);
const timestampKeys = new Set(["createdAt", "settledAt", "timeoutAt", "timeout", "nextRunAt", "lastRunAt"]);

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const waitForServer = async (
  server: Subprocess,
  output: { readonly stdout: Promise<string>; readonly stderr: Promise<string> },
) => {
  for (let attempt = 0; attempt < 50; attempt = attempt + 1) {
    const exitCode = await Promise.race([server.exited, sleep(0).then(() => undefined)]);
    if (Predicate.isNotUndefined(exitCode)) {
      throw new Error(
        `debug server exited (${exitCode})\nstdout:\n${await output.stdout}\nstderr:\n${await output.stderr}`,
      );
    }
    try {
      await fetch(serverUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          kind: "debug.snap",
          head: { corrId: `ready-${attempt}`, version: "2026-04-01" },
          data: {},
        }),
      });
      return;
    } catch {
      await sleep(100);
    }
  }
  kill(server);
  throw new Error(
    `debug server did not start at ${serverUrl}\nstdout:\n${await output.stdout}\nstderr:\n${await output.stderr}`,
  );
};

const kill = (process: Subprocess) => {
  process.kill();
};

const run = async (command: ReadonlyArray<string>, env: Record<string, string> = {}) => {
  const process = spawn(command, env);
  const [exitCode, stdout, stderr] = await Promise.all([
    process.exited,
    streamText(process.stdout),
    streamText(process.stderr),
  ]);
  if (exitCode !== 0) {
    throw new Error(`Command failed (${exitCode}): ${command.join(" ")}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
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
  const data = Schema.encodeSync(Protocol.DebugSnapSuccessResponse)(response).data;
  return stripTimestamps({
    ...data,
    tasks: data.tasks.map((task) => ({ ...task, version: "<lease-version>" })),
  });
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
  const stdout = streamText(worker.stdout);
  const stderr = streamText(worker.stderr);
  try {
    await sleep(2_000);
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
  const stdout = Predicate.isUndefined(worker) ? Promise.resolve("") : streamText(worker.stdout);
  const stderr = Predicate.isUndefined(worker) ? Promise.resolve("") : streamText(worker.stderr);
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

interface DriverParityCase {
  readonly name: string;
  readonly nativeCommand: ReadonlyArray<string>;
  readonly effectCommand: ReadonlyArray<string>;
  readonly pid: string;
}

const driverCases: ReadonlyArray<DriverParityCase> = [
  {
    name: "versions-targets",
    nativeCommand: ["bun", "test/interop/native-versions-targets-driver.js"],
    effectCommand: ["bun", "test/interop/effect-versions-targets-driver.ts"],
    pid: "versions-targets-worker",
  },
  {
    name: "context-apis",
    nativeCommand: ["bun", "test/interop/native-context-apis-driver.js"],
    effectCommand: ["bun", "test/interop/effect-context-apis-driver.ts"],
    pid: "context-apis-worker",
  },
  {
    name: "failure-modes",
    nativeCommand: ["bun", "test/interop/native-failure-modes-driver.js"],
    effectCommand: ["bun", "test/interop/effect-failure-modes-driver.ts"],
    pid: "failure-modes-worker",
  },
  {
    name: "timers-schedules",
    nativeCommand: ["bun", "test/interop/native-timers-schedules-driver.js"],
    effectCommand: ["bun", "test/interop/effect-timers-schedules-driver.ts"],
    pid: "timers-schedules-worker",
  },
];

const runDriverSide = async (options: {
  readonly name: string;
  readonly command: ReadonlyArray<string>;
  readonly pid: string;
}) => {
  const observed = stripTimestamps(
    parseJsonOutput(
      await run(options.command, {
        RESONATE_URL: serverUrl,
        RESONATE_GROUP: group,
        RESONATE_PID: options.pid,
      }),
    ),
  );
  return {
    observed,
    snapshot: await debugSnap(),
  };
};

const runDriverParityCase = async (entry: DriverParityCase) => {
  const native = await runDriverSide({ name: `${entry.name} native`, command: entry.nativeCommand, pid: entry.pid });
  await debugReset();
  const effect = await runDriverSide({ name: `${entry.name} effect`, command: entry.effectCommand, pid: entry.pid });

  expect(effect.observed).toEqual(native.observed);
  expect(effect.snapshot).toEqual(native.snapshot);
  expect(native).toMatchSnapshot(entry.name);
};

describe("debug snapshot parity", () => {
  it("matches the native TypeScript SDK fanout graph against a debug server", async () => {
    if (!commandExists("resonate")) {
      console.error("[DEBUG SNAPSHOT PARITY SKIPPED] resonate CLI not found; install it to run parity snapshots.");
      expect(commandExists("resonate")).toBe(false);
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
    const output = { stdout: streamText(server.stdout), stderr: streamText(server.stderr) };
    try {
      await waitForServer(server, output);
      for (const entry of cases) {
        await runParityCase(entry);
        await debugReset();
      }
      await runClientApiParity();
      await debugReset();
      for (const entry of driverCases) {
        await runDriverParityCase(entry);
        await debugReset();
      }
    } finally {
      kill(server);
    }
  }, 120_000);
});
