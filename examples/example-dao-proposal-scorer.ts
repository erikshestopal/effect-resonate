import { BunRuntime } from "@effect/platform-bun";
import { Config, Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

export const repo = "example-dao-proposal-scorer-ts";
export const functionName = "scoreProposal";
export const sampleArgs = [1] as const;
// Invoke after starting this worker: resonate invoke --server http://127.0.0.1:8001 --target poll://any@example-dao-proposal-scorer-ts --func scoreProposal --json-args '[1]' example-dao-proposal-scorer-ts-demo

const Payload = Schema.Finite;
const workflow = Resonate.function({ name: functionName, payload: Payload });
const App = Resonate.group(workflow);

const handlers = App.toLayer(
  App.of({
    [functionName]: (proposalId) =>
      Effect.gen(function* () {
        const ctx = yield* ResonateContext.ResonateContext;
        const votes = [
          { voter: "0x123", support: true, timestamp: 1 },
          { voter: "0x456", support: false, timestamp: 2 },
        ];
        yield* ctx.run({ effect: Effect.logInfo(`fetchVotes ${proposalId}`) });
        const reputations = votes.map((vote) => ({
          address: vote.voter,
          score: vote.support ? 80 : 35,
          eligible: true,
        }));
        yield* ctx.run({ effect: Effect.logInfo("getReputations") });
        const eligibleVotes = votes.filter((vote) =>
          reputations.some((rep) => rep.address === vote.voter && rep.eligible),
        );
        yield* ctx.run({ effect: Effect.logInfo("checkEligibility") });
        const weightedYesScore = eligibleVotes
          .filter((vote) => vote.support)
          .reduce((total, vote) => total + (reputations.find((rep) => rep.address === vote.voter)?.score ?? 0), 0);
        const weightedNoScore = eligibleVotes
          .filter((vote) => !vote.support)
          .reduce((total, vote) => total + (reputations.find((rep) => rep.address === vote.voter)?.score ?? 0), 0);
        const result = {
          proposalId,
          finalScore: weightedYesScore - weightedNoScore,
          breakdown: {
            totalVotes: eligibleVotes.length,
            yesVotes: eligibleVotes.filter((vote) => vote.support).length,
            noVotes: eligibleVotes.filter((vote) => !vote.support).length,
            weightedYesScore,
            weightedNoScore,
            netScore: weightedYesScore - weightedNoScore,
          },
          proofHash: `proposal-${proposalId}-${weightedYesScore}-${weightedNoScore}`,
        };
        yield* ctx.run({ effect: Effect.logInfo("calculateScore") });
        return result;
      }),
  }),
);

const worker = Layer.unwrap(
  Effect.gen(function* () {
    const url = yield* Config.string("RESONATE_URL").pipe(Config.withDefault("http://127.0.0.1:8001"));
    const groupName = yield* Config.string("RESONATE_GROUP").pipe(Config.withDefault(repo));
    const pidName = yield* Config.string("RESONATE_PID").pipe(Config.withDefault(`${repo}-worker`));
    const group = Protocol.WorkerGroup.make(groupName);
    const pid = Protocol.ProcessId.make(pidName);
    return Worker.layerHttp({ group: App, http: { url, group, pid, ttl: Duration.seconds(30) } }).pipe(
      Layer.provideMerge(handlers),
    );
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(Layer.launch(worker));
}
