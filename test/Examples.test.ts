import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema, SchemaParser } from "effect";
import { currentCodec } from "../src/Codec.ts";
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

const resolveString = async (id: string, value: string) => {
  const encoded = await Effect.runPromise(currentCodec.pipe(Effect.flatMap((codec) => codec.encode(value))));
  const response = await request(
    Protocol.PromiseSettleRequest.make({
      kind: "promise.settle",
      head: Protocol.RequestHead.make({
        corrId: Protocol.CorrelationId.make(`examples-settle-${id}-${Date.now()}`),
        version: Protocol.protocolVersion,
      }),
      data: { id: Protocol.PromiseId.make(id), state: Schema.Literal("resolved").make("resolved"), value: encoded },
    }),
  );
  expect(response.head.status).toBe(200);
};

const examples: ReadonlyArray<{ readonly name: string; readonly args: ReadonlyArray<unknown> }> = [
  { name: "foo", args: ["World"] },
  { name: "countdown", args: [2, 1] },
  { name: "sleepingWorkflow", args: [1] },
  { name: "generateReport", args: [123] },
  { name: "factorial", args: [4] },
  { name: "fooWorkflow", args: ["workflow-1"] },
  {
    name: "notifyAll",
    args: [{ orderId: "order-1", userId: "user-1", event: "created", message: "order created" }],
  },
  { name: "bookTrip", args: [{ tripId: "trip-1", shouldFail: false }] },
  {
    name: "importRecords",
    args: [
      {
        records: [
          { id: "a", value: 1 },
          { id: "b", value: 2 },
        ],
        batchSize: 1,
      },
    ],
  },
  {
    name: "exclusiveResourceAccess",
    args: [{ resource: "resource-1", workers: ["worker-a", "worker-b"], shouldCrash: false }],
  },
  {
    name: "rateLimitedBatch",
    args: [{ requests: [{ id: "req-1", endpoint: "/v1/orders", payload: "{}" }], ratePerSec: 1000 }],
  },
  {
    name: "processPayment",
    args: [{ event_id: "evt-1", type: "payment_intent.succeeded", amount: 42, currency: "USD", customer_id: "cus-1" }],
  },
  { name: "orderLifecycle", args: [{ orderId: "order-2", path: "deliver" }] },
  {
    name: "processEventStream",
    args: [{ userId: "user-1", events: [{ eventId: "event-1", type: "created", payload: { name: "Ada" } }] }],
  },
  {
    name: "processQueue",
    args: [{ jobs: [{ id: "job-1", priority: "critical", description: "ship", workMs: 1 }] }],
  },
];

describe("official TypeScript examples", () => {
  it("runs fifteen ports of the official TypeScript examples against the shipped server", async () => {
    if (Bun.which("resonate") === null) {
      console.error("[EXAMPLES SKIPPED] resonate CLI not found; install it to run examples.");
      expect(Bun.which("resonate")).toBeNull();
      return;
    }

    const group = `examples-${Date.now()}`;
    const target = `poll://any@${group}`;
    const server = spawn(["resonate", "dev", "--server-port", String(serverPort), "--observability-metrics-port", "0"]);
    await sleep(2_000);
    const worker = spawn(["bun", "examples/official-typescript.ts"], {
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
        if (example.name === "fooWorkflow") {
          await sleep(1_000);
          await resolveString(`${id}.human_approval`, "human_approval");
        }
        const promise = await waitForSettled(id);
        expect(promise.state).toBe("resolved");
        expect(await run(["resonate", "tree", "--server", serverUrl, id])).toContain(id);
      }
    } finally {
      kill(worker);
      kill(server);
      const workerLog = await new Response(worker.stdout).text();
      expect(workerLog).toContain("Hello World from bar");
      expect(workerLog).toContain("critical:job-1:ship");
    }
  }, 90_000);
});
