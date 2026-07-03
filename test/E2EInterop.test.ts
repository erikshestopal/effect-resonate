import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaParser } from "effect";
import { ResonateCodec, ResonateEncryptor } from "../src/Codec.ts";
import { decodeResponse, encodeRequest } from "../src/network/network.ts";
import * as Protocol from "../src/Protocol.ts";

const serverPort = 8011;
const serverUrl = `http://127.0.0.1:${serverPort}`;

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const kill = (process: Bun.Subprocess) => {
  process.kill();
};

const isPromiseGetSuccess = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);

const spawn = (command: Array<string>, env: Record<string, string> = {}) =>
  Bun.spawn(command, {
    env: { ...Bun.env, ...env },
    stdout: "pipe",
    stderr: "pipe",
  });

const run = async (command: Array<string>) => {
  const process = Bun.spawn(command, { stdout: "pipe", stderr: "pipe" });
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

const getPromise = async (id: string) => {
  const response = await request(
    Protocol.PromiseGetRequest.make({
      kind: "promise.get",
      head: Protocol.RequestHead.make({
        corrId: Protocol.CorrelationId.make(`get-${id}-${Date.now()}`),
        version: Protocol.protocolVersion,
      }),
      data: { id: Protocol.PromiseId.make(id) },
    }),
  );
  if (!isPromiseGetSuccess(response)) {
    throw new Error(`promise ${id} was not found`);
  }
  return response.data.promise;
};

const waitForSettled = async (id: string) => {
  for (let attempt = 0; attempt < 80; attempt = attempt + 1) {
    const promise = await getPromise(id);
    if (promise.state !== "pending") {
      return promise;
    }
    await sleep(250);
  }
  throw new Error(`promise ${id} did not settle`);
};

const decodeValue = (value: Protocol.Value) =>
  Effect.runPromise(
    ResonateCodec.pipe(
      Effect.flatMap((codec) => codec.decode(value)),
      Effect.provide(ResonateCodec.layerJson),
      Effect.provide(ResonateEncryptor.layerNoop),
    ),
  );

const resolveString = async (id: string, value: string) => {
  const encoded = await Effect.runPromise(
    ResonateCodec.pipe(
      Effect.flatMap((codec) => codec.encode(value)),
      Effect.provide(ResonateCodec.layerJson),
      Effect.provide(ResonateEncryptor.layerNoop),
    ),
  );
  const response = await request(
    Protocol.PromiseSettleRequest.make({
      kind: "promise.settle",
      head: Protocol.RequestHead.make({
        corrId: Protocol.CorrelationId.make(`settle-${id}-${Date.now()}`),
        version: Protocol.protocolVersion,
      }),
      data: { id: Protocol.PromiseId.make(id), state: Schema.Literal("resolved").make("resolved"), value: encoded },
    }),
  );
  expect(response.head.status).toBe(200);
};

const invoke = (id: string, func: string, args: ReadonlyArray<string>, target: string) =>
  run([
    "resonate",
    "invoke",
    "--server",
    serverUrl,
    "--func",
    func,
    "--target",
    target,
    ...args.flatMap((arg) => ["--arg", arg]),
    id,
  ]);

const createSchedule = (id: string, group: string) =>
  run([
    "resonate",
    "schedules",
    "create",
    "--server",
    serverUrl,
    "--cron",
    "* * * * *",
    "--promise-id",
    `${id}.{{.timestamp}}`,
    "--promise-timeout",
    "30s",
    "--promise-param",
    JSON.stringify({ data: { func: "EffectEcho", args: ["schedule"], version: 1 }, headers: {} }),
    "--promise-tags",
    JSON.stringify({ "resonate:target": `poll://any@${group}` }),
    id,
  ]);

describe("E2E interop", () => {
  it("runs the shipped-server quickstart, cross-SDK interop, external promises, schedules, and tree gate", async () => {
    if (Bun.which("resonate") === null) {
      console.error("[E2E SKIPPED] resonate CLI not found; install it to run shipped-server interop.");
      expect(Bun.which("resonate")).toBeNull();
      return;
    }

    const group = `e2e-${Date.now()}`;
    const target = `poll://any@${group}`;
    const server = spawn(["resonate", "dev", "--server-port", String(serverPort), "--observability-metrics-port", "0"]);
    await sleep(2_000);

    let effectWorker = spawn(["bun", "test/interop/effect-worker.ts"], {
      RESONATE_URL: serverUrl,
      RESONATE_GROUP: group,
      RESONATE_PID: "effect-1",
    });
    const nativeWorker = spawn(["bun", "test/interop/native-worker.js"], {
      RESONATE_URL: serverUrl,
      RESONATE_GROUP: group,
      RESONATE_PID: "native-1",
    });

    try {
      await sleep(2_000);

      await invoke(`${group}-countdown`, "Countdown", ["2", "1"], target);
      await sleep(1_250);
      kill(effectWorker);
      await sleep(1_000);
      effectWorker = spawn(["bun", "test/interop/effect-worker.ts"], {
        RESONATE_URL: serverUrl,
        RESONATE_GROUP: group,
        RESONATE_PID: "effect-2",
      });
      const countdown = await waitForSettled(`${group}-countdown`);
      expect(await decodeValue(countdown.value)).toBe("done");
      expect(await run(["resonate", "tree", "--server", serverUrl, `${group}-countdown`])).toContain(
        `${group}-countdown`,
      );

      await invoke(`${group}-effect-calls-native`, "EffectCallsNative", ["ping"], target);
      expect(await decodeValue((await waitForSettled(`${group}-effect-calls-native`)).value)).toBe("native:ping");

      await invoke(`${group}-native-calls-effect`, "NativeCallsEffect", ["pong"], target);
      expect(await decodeValue((await waitForSettled(`${group}-native-calls-effect`)).value)).toBe("effect:pong");

      await invoke(`${group}-effect-external`, "EffectAwaitsExternal", ["unused"], target);
      await sleep(1_000);
      await resolveString(`${group}-effect-external.approval`, "approved-by-native-shape");
      expect(await decodeValue((await waitForSettled(`${group}-effect-external`)).value)).toBe(
        "approved-by-native-shape",
      );

      await invoke(`${group}-native-external`, "NativeAwaitsExternal", [], target);
      await sleep(1_000);
      await resolveString(`${group}-native-external.0`, "approved-by-effect-shape");
      expect(await decodeValue((await waitForSettled(`${group}-native-external`)).value)).toBe(
        "approved-by-effect-shape",
      );

      await createSchedule(`${group}-schedule`, group);
      expect(await run(["resonate", "schedules", "get", "--server", serverUrl, `${group}-schedule`])).toContain(
        `${group}-schedule`,
      );
      await run(["resonate", "schedules", "delete", "--server", serverUrl, `${group}-schedule`]);
    } finally {
      kill(effectWorker);
      kill(nativeWorker);
      kill(server);
    }
  }, 60_000);
});
