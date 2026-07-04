import { Effect } from "effect";
import { Resonate, ResonateContext } from "effect-resonate";
import { generateReport, ReportPayload } from "./report.ts";

export const repo = "example-schedule-ts";
export const functionName = "generateReport";
export const sampleArgs = [123] as const;

export const workflow = Resonate.function({ name: functionName, payload: ReportPayload });
export const App = Resonate.group(workflow);

export const handlers = App.toLayer(
  App.of({
    [functionName]: (userId) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        return yield* ctx.run({ effect: generateReport(userId) });
      }),
  }),
);
