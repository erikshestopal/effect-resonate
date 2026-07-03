import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-tigerbeetle-account-creation-ts";
export const functionName = "createAccount";
export const sampleArgs = [{ accountId: "account-1", ledger: 1 }] as const;

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "example-tigerbeetle-account-creation-ts");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "example-tigerbeetle-account-creation-ts-worker");

const Payload = Schema.Struct({ accountId: Schema.String, ledger: Schema.Number });
const workflow = Resonate.function(functionName, { payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (input) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        results.push(
          yield* ctx.run(
            Effect.logInfo(`create account ${input.accountId}`).pipe(Effect.as(`create account ${input.accountId}`)),
          ),
        );
        yield* ctx.sleep(Duration.millis(1));
        return { repo, functionName, results };
      }),
  }),
);

const worker = Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers));

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
