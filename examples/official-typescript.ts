import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "official-examples");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "official-examples-worker");

const OrderEvent = Schema.Struct({
  orderId: Schema.String,
  userId: Schema.String,
  event: Schema.String,
  message: Schema.String,
});
const TripInput = Schema.Struct({ tripId: Schema.String, shouldFail: Schema.Boolean });
const ImportInput = Schema.Struct({
  records: Schema.Array(Schema.Struct({ id: Schema.String, value: Schema.Number })),
  batchSize: Schema.Number,
});
const MutexInput = Schema.Struct({
  resource: Schema.String,
  workers: Schema.Array(Schema.String),
  shouldCrash: Schema.Boolean,
});
const ApiRequest = Schema.Struct({ id: Schema.String, endpoint: Schema.String, payload: Schema.String });
const RateLimitInput = Schema.Struct({ requests: Schema.Array(ApiRequest), ratePerSec: Schema.Number });
const WebhookEvent = Schema.Struct({
  event_id: Schema.String,
  type: Schema.String,
  amount: Schema.Number,
  currency: Schema.String,
  customer_id: Schema.String,
});
const OrderLifecycleInput = Schema.Struct({
  orderId: Schema.String,
  path: Schema.Literals(["deliver", "cancel", "crash"]),
});
const UserEvent = Schema.Struct({
  eventId: Schema.String,
  type: Schema.String,
  payload: Schema.Record(Schema.String, Schema.Unknown),
});
const EventStreamInput = Schema.Struct({ userId: Schema.String, events: Schema.Array(UserEvent) });
const Job = Schema.Struct({
  id: Schema.String,
  priority: Schema.Literals(["critical", "high", "normal", "low"]),
  description: Schema.String,
  workMs: Schema.Number,
});
const QueueInput = Schema.Struct({ jobs: Schema.Array(Job) });

const foo = Resonate.function("foo", { payload: Schema.String });
const countdown = Resonate.function("countdown", { payload: Schema.Tuple([Schema.Number, Schema.Number]) });
const sleepingWorkflow = Resonate.function("sleepingWorkflow", { payload: Schema.Number });
const generateReport = Resonate.function("generateReport", { payload: Schema.Number });
const factorial = Resonate.function("factorial", { payload: Schema.Number });
const fooWorkflow = Resonate.function("fooWorkflow", { payload: Schema.String });
const notifyAll = Resonate.function("notifyAll", { payload: OrderEvent });
const bookTrip = Resonate.function("bookTrip", { payload: TripInput });
const importRecords = Resonate.function("importRecords", { payload: ImportInput });
const exclusiveResourceAccess = Resonate.function("exclusiveResourceAccess", { payload: MutexInput });
const rateLimitedBatch = Resonate.function("rateLimitedBatch", { payload: RateLimitInput });
const processPayment = Resonate.function("processPayment", { payload: WebhookEvent });
const orderLifecycle = Resonate.function("orderLifecycle", { payload: OrderLifecycleInput });
const processEventStream = Resonate.function("processEventStream", { payload: EventStreamInput });
const processQueue = Resonate.function("processQueue", { payload: QueueInput });

const Approval = Resonate.promise("human_approval", { success: Schema.String });

const App = Resonate.group(
  foo,
  countdown,
  sleepingWorkflow,
  generateReport,
  factorial,
  fooWorkflow,
  notifyAll,
  bookTrip,
  importRecords,
  exclusiveResourceAccess,
  rateLimitedBatch,
  processPayment,
  orderLifecycle,
  processEventStream,
  processQueue,
);

const runStep = (message: string) => Effect.logInfo(message).pipe(Effect.as(message));

const handlers = App.toLayer(
  App.of({
    foo: (greetee) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const first = `Hello ${greetee} from foo`;
        const second = yield* ctx.run(runStep(`Hello ${greetee} from bar`));
        const third = yield* ctx.run(runStep(`Hello ${greetee} from baz`));
        return `${first}; ${second}; ${third}`;
      }),
    countdown: (count, delay) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        for (let remaining = count; remaining > 0; remaining = remaining - 1) {
          yield* ctx.run(runStep(`Countdown: ${remaining}`));
          yield* ctx.sleep(Duration.seconds(delay));
        }
        return yield* ctx.run(runStep("Done!"));
      }),
    sleepingWorkflow: (ms) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(runStep(`Sleeping for ${ms} milliseconds`));
        yield* ctx.sleep(Duration.millis(ms));
        return `Slept for ${ms / 1000} seconds`;
      }),
    generateReport: (userId) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        return yield* ctx.run(runStep(`Generated daily report for user ${userId}`));
      }),
    factorial: (n) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        if (n <= 1) {
          return 1;
        }
        const next = yield* ctx.rpc(factorial, [n - 1], { target: group });
        return n * Number(next);
      }),
    fooWorkflow: (workflowId) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const approval = yield* ctx.promise(Approval);
        yield* ctx.run(runStep(`send approval email for ${workflowId} with promise ${approval.id}`));
        return yield* approval.await;
      }),
    notifyAll: (event) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const email = yield* ctx.beginRun(runStep(`email ${event.userId}:${event.message}`));
        const sms = yield* ctx.beginRun(runStep(`sms ${event.userId}:${event.message}`));
        const slack = yield* ctx.beginRun(runStep(`slack ${event.orderId}:${event.event}`));
        const push = yield* ctx.beginRun(runStep(`push ${event.orderId}:${event.event}`));
        return yield* ctx.all([email.await, sms.await, slack.await, push.await]);
      }),
    bookTrip: ({ tripId, shouldFail }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const flightId = yield* ctx.run(runStep(`flight-${tripId}`));
        const hotelId = yield* ctx.run(runStep(`hotel-${tripId}`));
        if (shouldFail) {
          yield* ctx.run(runStep(`cancel-${hotelId}`));
          yield* ctx.run(runStep(`cancel-${flightId}`));
          return { status: "failed", tripId, compensated: true };
        }
        const carId = yield* ctx.run(runStep(`car-${tripId}`));
        return { status: "booked", tripId, flightId, hotelId, carId };
      }),
    importRecords: ({ records, batchSize }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const batches: Array<unknown> = [];
        for (let index = 0; index < records.length; index = index + batchSize) {
          const batch = records.slice(index, index + batchSize);
          batches.push(yield* ctx.run(runStep(`processed batch ${index / batchSize}:${batch.length}`)));
        }
        return { totalRecords: records.length, batchCount: batches.length, batches };
      }),
    exclusiveResourceAccess: ({ resource, workers }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const processed: Array<unknown> = [];
        for (const worker of workers) {
          processed.push(yield* ctx.run(runStep(`${worker} accessed ${resource}`)));
        }
        return { resource, processed };
      }),
    rateLimitedBatch: ({ requests, ratePerSec }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const responses: Array<unknown> = [];
        for (const request of requests) {
          responses.push(yield* ctx.run(runStep(`${request.id}:${request.endpoint}:${request.payload}`)));
          yield* ctx.sleep(Duration.millis(Math.floor(1000 / ratePerSec)));
        }
        return { totalRequests: requests.length, completed: responses.length, ratePerSec, responses };
      }),
    processPayment: (event) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(runStep(`validated ${event.event_id}`));
        const chargeId = yield* ctx.run(runStep(`charged ${event.amount} ${event.currency}`));
        yield* ctx.run(runStep(`receipt ${event.customer_id}`));
        yield* ctx.run(runStep(`ledger ${event.event_id}`));
        return { event_id: event.event_id, charge_id: chargeId, status: "captured", amount: event.amount };
      }),
    orderLifecycle: ({ orderId, path }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const history: Array<unknown> = [];
        history.push(yield* ctx.run(runStep(`${orderId}:created`)));
        history.push(yield* ctx.run(runStep(`${orderId}:confirmed`)));
        if (path === "cancel") {
          history.push(yield* ctx.run(runStep(`${orderId}:cancelled`)));
          history.push(yield* ctx.run(runStep(`${orderId}:refunded`)));
          return { orderId, finalState: "refunded", history };
        }
        history.push(yield* ctx.run(runStep(`${orderId}:shipped`)));
        history.push(yield* ctx.run(runStep(`${orderId}:delivered`)));
        return { orderId, finalState: "delivered", history };
      }),
    processEventStream: ({ userId, events }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        let projection = { userId, eventsProcessed: 0, lastEvent: "" };
        for (const event of events) {
          const lastEvent = yield* ctx.run(runStep(`${event.eventId}:${event.type}`));
          projection = { userId, eventsProcessed: projection.eventsProcessed + 1, lastEvent: String(lastEvent) };
        }
        return { userId, eventsProcessed: events.length, finalProjection: projection };
      }),
    processQueue: ({ jobs }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const weights = { critical: 0, high: 1, normal: 2, low: 3 };
        const sorted = [...jobs].sort((left, right) => weights[left.priority] - weights[right.priority]);
        const completed: Array<unknown> = [];
        for (let index = 0; index < sorted.length; index = index + 2) {
          const chunk = sorted.slice(index, index + 2);
          const handles = yield* Effect.forEach(chunk, (job) =>
            ctx.beginRun(runStep(`${job.priority}:${job.id}:${job.description}`)),
          );
          completed.push(...(yield* ctx.all(handles.map((handle) => handle.await))));
        }
        return { totalJobs: jobs.length, completedJobs: completed.length, processingOrder: completed };
      }),
  }),
);

if (import.meta.main) {
  BunRuntime.runMain(
    Layer.launch(
      Worker.layerHttp(App, { url, group, pid, ttl: Duration.seconds(5) }).pipe(Layer.provideMerge(handlers)),
    ),
  );
}
