import { Effect } from "effect";
import { handleProcessDocument } from "./handler.ts";
import { sampleArgs } from "./workflow.ts";
export const localDemo = handleProcessDocument(sampleArgs[0]).pipe(
  Effect.tap((response) => Effect.logInfo(response.body)),
);
