import { Effect } from "effect";
export const probe = (uuid: string) => Effect.succeed({ uuid, status: "simulated", value: null });
