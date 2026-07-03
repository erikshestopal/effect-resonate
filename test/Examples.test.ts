import { describe, expect, it } from "@effect/vitest";
import { Effect, SchemaParser } from "effect";
import * as Protocol from "../src/Protocol.ts";
import { decodeResponse, encodeRequest } from "../src/Network.ts";

const serverPort = 8012;
const serverUrl = `http://127.0.0.1:${serverPort}`;

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const kill = (process: Bun.Subprocess) => {
  process.kill();
};

const spawn = (command: Array<string>, env: Record<string, string> = {}) =>
  Bun.spawn(command, {
    env: { ...process.env, ...env },
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

const isPromiseGetSuccess = SchemaParser.is(Protocol.PromiseGetResponse.members[0]);

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
        corrId: Protocol.CorrelationId.make(`examples-get-${id}-${Date.now()}`),
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

const examples: ReadonlyArray<{ readonly name: string; readonly args: ReadonlyArray<unknown> }> = [
  { name: "helloWorld", args: ["World"] },
  { name: "durableSleep", args: [1] },
  { name: "sagaBooking", args: [{ tripId: "trip-1", shouldFail: false }] },
  { name: "fanOutFanIn", args: [{ orderId: "order-1", email: "a@example.com", phone: "+15555550100" }] },
  { name: "distributedMutex", args: [["worker-a", "worker-b"]] },
  { name: "batchProcessor", args: [{ records: ["a", "b", "c"], batchSize: 2 }] },
  {
    name: "priorityQueue",
    args: [
      {
        jobs: [
          { id: "low", priority: 1 },
          { id: "high", priority: 10 },
        ],
      },
    ],
  },
  { name: "rateLimiter", args: [["req-1", "req-2"]] },
  { name: "stateMachine", args: [{ orderId: "order-2", path: "deliver" }] },
  { name: "foodDelivery", args: [{ orderId: "food-1", hasDriver: true }] },
  { name: "eventSourcing", args: [{ userId: "user-1", events: ["created", "updated"] }] },
  { name: "sessionLifecycle", args: [{ sessionId: "session-1", userId: "user-1", activities: ["click"] }] },
  { name: "asyncHttpApi", args: ["request-1"] },
  { name: "scheduleReport", args: ["user-123"] },
  { name: "recursiveFactorial", args: [4] },
  { name: "loadBalancedCompute", args: [7] },
  { name: "webhookPayment", args: [{ eventId: "evt-1", amount: 42 }] },
  { name: "healthMonitor", args: [{ services: ["api", "db"], iterations: 2 }] },
  { name: "agentOrchestration", args: [{ topic: "resonate" }] },
  { name: "imagePipeline", args: [{ prompt: "cat" }] },
];

describe("official examples catalog", () => {
  it("runs at least twenty converted examples against the shipped server", async () => {
    if (Bun.which("resonate") === null) {
      console.error("[EXAMPLES SKIPPED] resonate CLI not found; install it to run examples.");
      expect(Bun.which("resonate")).toBeNull();
      return;
    }

    const group = `examples-${Date.now()}`;
    const target = `poll://any@${group}`;
    const server = spawn(["resonate", "dev", "--server-port", String(serverPort), "--observability-metrics-port", "0"]);
    await sleep(2_000);
    const worker = spawn(["bun", "examples/catalog.ts"], {
      RESONATE_URL: serverUrl,
      RESONATE_GROUP: group,
      RESONATE_PID: "examples-1",
    });

    try {
      await sleep(2_000);
      for (const example of examples) {
        const id = `${group}-${example.name}`;
        await run([
          "resonate",
          "invoke",
          "--server",
          serverUrl,
          "--func",
          example.name,
          "--target",
          target,
          "--json-args",
          JSON.stringify(example.args),
          id,
        ]);
        const promise = await waitForSettled(id);
        expect(promise.state).toBe("resolved");
        expect(await run(["resonate", "tree", "--server", serverUrl, id])).toContain(id);
      }
    } finally {
      kill(worker);
      kill(server);
      const workerLog = await new Response(worker.stdout).text();
      expect(workerLog).toContain("hello World");
      expect(workerLog).toContain("photorealistic cat");
    }
  }, 90_000);
});
