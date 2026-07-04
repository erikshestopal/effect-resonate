import { describe, expect, it } from "@effect/vitest";
import { Effect, Option, Predicate, Schema, SchemaParser } from "effect";
import { decodeResponse, encodeRequest } from "../src/network/network.ts";
import * as Protocol from "../src/Protocol.ts";

const serverPort = 8013;
const serverUrl = `http://127.0.0.1:${serverPort}`;
const group = "debug-snapshot-parity";

type DebugState = (typeof Protocol.DebugSnapResponse.members)[0]["Type"]["data"];

const isPromiseGetSuccess = SchemaParser.is(Protocol.PromiseGetSuccessResponse);
const isDebugSnapSuccess = SchemaParser.is(Protocol.DebugSnapSuccessResponse);
const isDebugResetSuccess = SchemaParser.is(Protocol.DebugResetResponse.members[0]);
const JsonFromBase64 = Schema.StringFromBase64.pipe(Schema.decodeTo(Schema.fromJsonString(Schema.Unknown)));

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

const run = async (command: ReadonlyArray<string>) => {
  const process = Bun.spawn([...command], { stdout: "pipe", stderr: "pipe" });
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

const debugSnap = async (): Promise<DebugState> => {
  const response = await request(
    Protocol.DebugSnapRequest.make({ kind: "debug.snap", head: requestHead(`snap-${Date.now()}`), data: {} }),
  );
  if (!isDebugSnapSuccess(response)) {
    throw new Error(`debug.snap failed: ${JSON.stringify(response.data)}`);
  }
  return response.data;
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

const replaceRoot = (value: string, root: Protocol.PromiseId): string => value.replaceAll(root, "<root>");

const normalizeJson = (value: unknown, root: Protocol.PromiseId): unknown => {
  if (Predicate.isString(value)) {
    return replaceRoot(value, root);
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeJson(item, root));
  }
  if (Predicate.isObject(value)) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeJson(item, root)]));
  }
  return value;
};

const optionalValue = <A>(value: A | Option.Option<A> | undefined): A | undefined =>
  Option.isOption(value) ? Option.getOrUndefined(value) : value;

const normalizeValue = (value: Protocol.Value, root: Protocol.PromiseId) => {
  const data = optionalValue(value.data);
  const headers = optionalValue(value.headers);
  return {
    ...(Predicate.isNotUndefined(data)
      ? { data: normalizeJson(Schema.decodeUnknownSync(JsonFromBase64)(data), root) }
      : {}),
    ...(Predicate.isNotUndefined(headers) ? { headers } : {}),
  };
};

const sortBy = <A>(items: ReadonlyArray<A>, key: (value: A) => string): ReadonlyArray<A> =>
  [...items].sort((left, right) => key(left).localeCompare(key(right)));

const normalizePromise = (promise: Protocol.PromiseRecord, root: Protocol.PromiseId) => {
  const tags = Schema.encodeSync(Protocol.TagsFromWire)(promise.tags);
  return {
    id: replaceRoot(promise.id, root),
    param: normalizeValue(promise.param, root),
    state: promise.state,
    tags: Object.fromEntries(
      sortBy(
        Object.entries(tags).map(([key, value]) => [key, replaceRoot(value, root)] as const),
        ([key]) => key,
      ),
    ),
    value: normalizeValue(promise.value, root),
  };
};

const normalizeTask = (task: Protocol.TaskRecord, root: Protocol.PromiseId) => ({
  id: replaceRoot(task.id, root),
  resumes: task.resumes,
  state: task.state,
  version: task.version,
});

const normalizeSnapshot = (state: DebugState, root: Protocol.PromiseId) => ({
  callbacks: sortBy(
    state.callbacks.map((callback) => ({
      awaiter: replaceRoot(callback.awaiter, root),
      awaited: replaceRoot(callback.awaited, root),
    })),
    (callback) => `${callback.awaiter}:${callback.awaited}`,
  ),
  listeners: sortBy(
    (state.listeners ?? []).map((listener) => ({
      id: replaceRoot(listener.id, root),
      address: replaceRoot(listener.address, root),
    })),
    (listener) => `${listener.id}:${listener.address}`,
  ),
  messages: sortBy(
    state.messages.map((message) => ({
      address: replaceRoot(message.address, root),
      kind: message.message.kind,
    })),
    (message) => `${message.address}:${message.kind}`,
  ),
  promises: sortBy(
    state.promises.map((promise) => normalizePromise(promise, root)),
    (promise) => promise.id,
  ),
  promiseTimeouts: sortBy(
    state.promiseTimeouts.map((timeout) => ({ id: replaceRoot(timeout.id, root), timeout: "<timestamp>" })),
    (timeout) => timeout.id,
  ),
  tasks: sortBy(
    state.tasks.map((task) => normalizeTask(task, root)),
    (task) => task.id,
  ),
  taskTimeouts: sortBy(
    state.taskTimeouts.map((timeout) => ({
      id: replaceRoot(timeout.id, root),
      type: timeout.type,
      timeout: "<timestamp>",
    })),
    (timeout) => `${timeout.id}:${timeout.type}`,
  ),
});

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
    return normalizeSnapshot(await debugSnap(), options.id);
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
    } finally {
      kill(server);
    }
  }, 120_000);
});
