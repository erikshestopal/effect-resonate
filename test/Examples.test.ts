import { describe, expect, it } from "@effect/vitest";
import { Effect, SchemaParser } from "effect";
import * as Protocol from "../src/Protocol.ts";
import { decodeResponse, encodeRequest } from "../src/network/network.ts";

const serverPort = 8012;
const serverUrl = `http://127.0.0.1:${serverPort}`;

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

const kill = (process: Bun.Subprocess) => {
  process.kill();
};

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

const examples: ReadonlyArray<{ readonly file: string; readonly importPath: string }> = [
  { file: "examples/example-ai-image-pipeline.ts", importPath: "../examples/example-ai-image-pipeline.ts" },
  { file: "examples/example-async-http-api.ts", importPath: "../examples/example-async-http-api.ts" },
  { file: "examples/example-async-rpc.ts", importPath: "../examples/example-async-rpc.ts" },
  { file: "examples/example-aws-lambda.ts", importPath: "../examples/example-aws-lambda.ts" },
  { file: "examples/example-batch-processor.ts", importPath: "../examples/example-batch-processor.ts" },
  { file: "examples/example-bluesky-scraper.ts", importPath: "../examples/example-bluesky-scraper.ts" },
  { file: "examples/example-browser-worker.ts", importPath: "../examples/example-browser-worker.ts" },
  { file: "examples/example-chess-hero-gcp.ts", importPath: "../examples/example-chess-hero-gcp.ts" },
  { file: "examples/example-countdown-cloudflare.ts", importPath: "../examples/example-countdown-cloudflare.ts" },
  { file: "examples/example-countdown-gcp.ts", importPath: "../examples/example-countdown-gcp.ts" },
  { file: "examples/example-countdown-supabase.ts", importPath: "../examples/example-countdown-supabase.ts" },
  { file: "examples/example-countdown-web.ts", importPath: "../examples/example-countdown-web.ts" },
  { file: "examples/example-countdown.ts", importPath: "../examples/example-countdown.ts" },
  { file: "examples/example-dao-proposal-scorer.ts", importPath: "../examples/example-dao-proposal-scorer.ts" },
  { file: "examples/example-distributed-mutex.ts", importPath: "../examples/example-distributed-mutex.ts" },
  { file: "examples/example-durable-chatbot.ts", importPath: "../examples/example-durable-chatbot.ts" },
  { file: "examples/example-durable-entity.ts", importPath: "../examples/example-durable-entity.ts" },
  { file: "examples/example-durable-sleep.ts", importPath: "../examples/example-durable-sleep.ts" },
  { file: "examples/example-ecommerce-application.ts", importPath: "../examples/example-ecommerce-application.ts" },
  { file: "examples/example-encryption.ts", importPath: "../examples/example-encryption.ts" },
  { file: "examples/example-event-sourcing.ts", importPath: "../examples/example-event-sourcing.ts" },
  { file: "examples/example-express-integration.ts", importPath: "../examples/example-express-integration.ts" },
  { file: "examples/example-fan-out-fan-in.ts", importPath: "../examples/example-fan-out-fan-in.ts" },
  { file: "examples/example-food-delivery.ts", importPath: "../examples/example-food-delivery.ts" },
  {
    file: "examples/example-hackernews-research-agent.ts",
    importPath: "../examples/example-hackernews-research-agent.ts",
  },
  { file: "examples/example-hello-world.ts", importPath: "../examples/example-hello-world.ts" },
  { file: "examples/example-human-in-the-loop.ts", importPath: "../examples/example-human-in-the-loop.ts" },
  { file: "examples/example-infinite-workflow.ts", importPath: "../examples/example-infinite-workflow.ts" },
  { file: "examples/example-kafka-worker.ts", importPath: "../examples/example-kafka-worker.ts" },
  { file: "examples/example-load-balancing.ts", importPath: "../examples/example-load-balancing.ts" },
  { file: "examples/example-mcp-tools.ts", importPath: "../examples/example-mcp-tools.ts" },
  {
    file: "examples/example-multi-agent-orchestration.ts",
    importPath: "../examples/example-multi-agent-orchestration.ts",
  },
  { file: "examples/example-nextjs-ecommerce.ts", importPath: "../examples/example-nextjs-ecommerce.ts" },
  { file: "examples/example-nextjs-integration.ts", importPath: "../examples/example-nextjs-integration.ts" },
  { file: "examples/example-node-drain-orchestrator.ts", importPath: "../examples/example-node-drain-orchestrator.ts" },
  {
    file: "examples/example-openai-deep-research-agent-cloudflare.ts",
    importPath: "../examples/example-openai-deep-research-agent-cloudflare.ts",
  },
  {
    file: "examples/example-openai-deep-research-agent-gcp.ts",
    importPath: "../examples/example-openai-deep-research-agent-gcp.ts",
  },
  {
    file: "examples/example-openai-deep-research-agent-supabase.ts",
    importPath: "../examples/example-openai-deep-research-agent-supabase.ts",
  },
  {
    file: "examples/example-openai-deep-research-agent.ts",
    importPath: "../examples/example-openai-deep-research-agent.ts",
  },
  { file: "examples/example-priority-queue.ts", importPath: "../examples/example-priority-queue.ts" },
  { file: "examples/example-quickstart.ts", importPath: "../examples/example-quickstart.ts" },
  { file: "examples/example-rate-limiter.ts", importPath: "../examples/example-rate-limiter.ts" },
  { file: "examples/example-recursive-factorial.ts", importPath: "../examples/example-recursive-factorial.ts" },
  { file: "examples/example-saga-booking.ts", importPath: "../examples/example-saga-booking.ts" },
  { file: "examples/example-schedule.ts", importPath: "../examples/example-schedule.ts" },
  { file: "examples/example-state-machine.ts", importPath: "../examples/example-state-machine.ts" },
  { file: "examples/example-supabase-edge.ts", importPath: "../examples/example-supabase-edge.ts" },
  {
    file: "examples/example-tigerbeetle-account-creation.ts",
    importPath: "../examples/example-tigerbeetle-account-creation.ts",
  },
  { file: "examples/example-token-auth.ts", importPath: "../examples/example-token-auth.ts" },
  { file: "examples/example-webhook-handler.ts", importPath: "../examples/example-webhook-handler.ts" },
  { file: "examples/templated-agent.ts", importPath: "../examples/templated-agent.ts" },
];

interface RunningExample {
  readonly file: string;
  readonly repo: string;
  readonly functionName: string;
  readonly sampleArgs: ReadonlyArray<unknown>;
  readonly group: string;
  readonly target: string;
  readonly worker: Bun.Subprocess;
  readonly stdout: Promise<string>;
  readonly stderr: Promise<string>;
}

const startExample = async (example: (typeof examples)[number]): Promise<RunningExample> => {
  const module = (await import(example.importPath)) as {
    readonly repo: string;
    readonly functionName: string;
    readonly sampleArgs: ReadonlyArray<unknown>;
  };
  const group = `examples-${Date.now()}-${module.repo}`;
  const target = `poll://any@${group}`;
  const worker = spawn(["bun", example.file], {
    RESONATE_URL: serverUrl,
    RESONATE_GROUP: group,
    RESONATE_PID: `${module.repo}-worker`,
  });
  const stdout = new Response(worker.stdout).text();
  const stderr = new Response(worker.stderr).text();

  return {
    file: example.file,
    repo: module.repo,
    functionName: module.functionName,
    sampleArgs: module.sampleArgs,
    group,
    target,
    worker,
    stdout,
    stderr,
  };
};

const runExample = async (example: RunningExample) => {
  try {
    const id = `${example.group}-run`;
    await run([
      "resonate",
      "invoke",
      "--server",
      serverUrl,
      "--func",
      example.functionName,
      "--target",
      example.target,
      "--json-args",
      JSON.stringify(example.sampleArgs),
      id,
    ]);
    const promise = await waitForSettled(id);
    expect(promise.state).toBe("resolved");
    expect(await run(["resonate", "tree", "--server", serverUrl, id])).toContain(id);
  } catch (cause) {
    kill(example.worker);
    throw new Error(`${example.file} failed\nstdout:\n${await example.stdout}\nstderr:\n${await example.stderr}`, {
      cause,
    });
  }
};

describe("official TypeScript example repos", () => {
  it("runs one self-contained Effect port per official TypeScript example repo against the shipped server", async () => {
    if (Bun.which("resonate") === null) {
      console.error("[EXAMPLES SKIPPED] resonate CLI not found; install it to run examples.");
      expect(Bun.which("resonate")).toBeNull();
      return;
    }

    const server = spawn(["resonate", "dev", "--server-port", String(serverPort), "--observability-metrics-port", "0"]);
    await sleep(2_000);

    try {
      const running = await Promise.all(examples.map(startExample));
      try {
        await sleep(2_000);
        const results = await Promise.allSettled(running.map(runExample));
        const failures = results.filter((result) => result.status === "rejected");
        if (failures.length > 0) {
          throw new AggregateError(
            failures.map((failure) => failure.reason),
            `${failures.length} examples failed`,
          );
        }
      } finally {
        for (const example of running) {
          kill(example.worker);
        }
      }
    } finally {
      kill(server);
    }
  }, 90_000);
});
