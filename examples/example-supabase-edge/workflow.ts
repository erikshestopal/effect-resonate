import { DateTime, Effect, Schema, SchemaParser } from "effect";
import { Resonate } from "effect-resonate";
export const repo = "example-supabase-edge-ts";
export const functionName = "onboardUser";
export const UserRecord = Schema.Struct({
  id: Schema.String,
  email: Schema.String,
  full_name: Schema.String,
  plan: Schema.Literals(["free", "pro"]),
  created_at: Schema.String,
});
export const OnboardingResult = Schema.Struct({
  userId: Schema.String,
  email: Schema.String,
  emailSent: Schema.Boolean,
  trialProvisioned: Schema.Boolean,
  crmUpdated: Schema.Boolean,
  completedAt: Schema.String,
});
export const sampleArgs = [
  UserRecord.make({
    id: "usr_12345678",
    email: "alice@example.com",
    full_name: "Alice Chen",
    plan: "free",
    created_at: "2026-01-01T00:00:00.000Z",
  }),
] as const;
export const workflow = Resonate.function({ name: functionName, payload: UserRecord });
export const App = Resonate.group(workflow);
const validateUser = (user: typeof UserRecord.Type) =>
  Effect.logInfo(`[validate] user ${user.id} (${user.email}) OK`).pipe(Effect.as(true));
const sendWelcomeEmail = (user: typeof UserRecord.Type) =>
  Effect.logInfo(`[email] Sending welcome email to ${user.email}`).pipe(Effect.as(true));
const provisionTrial = (user: typeof UserRecord.Type) =>
  Effect.logInfo(`[provision] Provisioning ${user.plan} trial for ${user.id}`).pipe(
    Effect.as(`ws_${user.id.slice(0, 8)}`),
  );
const notifyCRM = (user: typeof UserRecord.Type, workspaceId: string) =>
  Effect.logInfo(`[crm] Syncing user ${user.id} -> CRM (${workspaceId})`).pipe(Effect.as(true));
export const handlers = App.toLayer(
  App.of({
    [functionName]: (user) =>
      Effect.gen(function* (): Effect.fn.Return<typeof OnboardingResult.Type, unknown, Resonate.Context> {
        const ctx = yield* Resonate.Context;
        yield* ctx.run({ name: "validate", effect: validateUser(user) });
        yield* ctx.run({ name: "email", effect: sendWelcomeEmail(user) });
        const workspaceId = yield* ctx
          .run({ name: "provision", effect: provisionTrial(user) })
          .pipe(Effect.flatMap(SchemaParser.decodeUnknownEffect(Schema.String)));
        yield* ctx.run({ name: "crm", effect: notifyCRM(user, workspaceId) });
        const completedAt = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
        return OnboardingResult.make({
          userId: user.id,
          email: user.email,
          emailSent: true,
          trialProvisioned: true,
          crmUpdated: true,
          completedAt,
        });
      }),
  }),
);
