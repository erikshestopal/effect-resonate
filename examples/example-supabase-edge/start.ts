import { Effect } from "effect";
export const start = (uuid: string, func: string, args: ReadonlyArray<unknown>) =>
  Effect.succeed({ uuid, func, args, forwarded: true });
