import { handleWebhook } from "./flows.ts";
import { sampleArgs } from "./workflow.ts";
export const localDemo = handleWebhook({ type: "INSERT", table: "users", schema: "public", record: sampleArgs[0] });
