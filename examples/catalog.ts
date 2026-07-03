import { BunRuntime } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schema } from "effect";
import { Protocol, Resonate, ResonateContext, Worker } from "effect-resonate";

const url = process.env.RESONATE_URL ?? "http://127.0.0.1:8001";
const group = Protocol.WorkerGroup.make(process.env.RESONATE_GROUP ?? "examples");
const pid = Protocol.ProcessId.make(process.env.RESONATE_PID ?? "examples-worker");

const OrderEvent = Schema.Struct({ orderId: Schema.String, email: Schema.String, phone: Schema.String });
const Trip = Schema.Struct({ tripId: Schema.String, shouldFail: Schema.Boolean });
const Records = Schema.Struct({ records: Schema.Array(Schema.String), batchSize: Schema.Number });
const Jobs = Schema.Struct({ jobs: Schema.Array(Schema.Struct({ id: Schema.String, priority: Schema.Number })) });
const OrderPath = Schema.Struct({ orderId: Schema.String, path: Schema.Literals(["deliver", "cancel"]) });
const FoodOrder = Schema.Struct({ orderId: Schema.String, hasDriver: Schema.Boolean });
const Events = Schema.Struct({ userId: Schema.String, events: Schema.Array(Schema.String) });
const Session = Schema.Struct({
  sessionId: Schema.String,
  userId: Schema.String,
  activities: Schema.Array(Schema.String),
});
const Webhook = Schema.Struct({ eventId: Schema.String, amount: Schema.Number });
const Monitor = Schema.Struct({ services: Schema.Array(Schema.String), iterations: Schema.Number });
const Topic = Schema.Struct({ topic: Schema.String });
const Image = Schema.Struct({ prompt: Schema.String });

const helloWorld = Resonate.function("helloWorld", { payload: Schema.String });
const durableSleep = Resonate.function("durableSleep", { payload: Schema.Number });
const sagaBooking = Resonate.function("sagaBooking", { payload: Trip });
const fanOutFanIn = Resonate.function("fanOutFanIn", { payload: OrderEvent });
const distributedMutex = Resonate.function("distributedMutex", { payload: Schema.Array(Schema.String) });
const batchProcessor = Resonate.function("batchProcessor", { payload: Records });
const priorityQueue = Resonate.function("priorityQueue", { payload: Jobs });
const rateLimiter = Resonate.function("rateLimiter", { payload: Schema.Array(Schema.String) });
const stateMachine = Resonate.function("stateMachine", { payload: OrderPath });
const foodDelivery = Resonate.function("foodDelivery", { payload: FoodOrder });
const eventSourcing = Resonate.function("eventSourcing", { payload: Events });
const sessionLifecycle = Resonate.function("sessionLifecycle", { payload: Session });
const asyncHttpApi = Resonate.function("asyncHttpApi", { payload: Schema.String });
const scheduleReport = Resonate.function("scheduleReport", { payload: Schema.String });
const recursiveFactorial = Resonate.function("recursiveFactorial", { payload: Schema.Number });
const loadBalancedCompute = Resonate.function("loadBalancedCompute", { payload: Schema.Number });
const webhookPayment = Resonate.function("webhookPayment", { payload: Webhook });
const healthMonitor = Resonate.function("healthMonitor", { payload: Monitor });
const agentOrchestration = Resonate.function("agentOrchestration", { payload: Topic });
const imagePipeline = Resonate.function("imagePipeline", { payload: Image });

const App = Resonate.group(
  helloWorld,
  durableSleep,
  sagaBooking,
  fanOutFanIn,
  distributedMutex,
  batchProcessor,
  priorityQueue,
  rateLimiter,
  stateMachine,
  foodDelivery,
  eventSourcing,
  sessionLifecycle,
  asyncHttpApi,
  scheduleReport,
  recursiveFactorial,
  loadBalancedCompute,
  webhookPayment,
  healthMonitor,
  agentOrchestration,
  imagePipeline,
);

const record = (message: string) =>
  Effect.sync(() => {
    console.log(message);
    return message;
  });

const handlers = App.toLayer(
  App.of({
    helloWorld: (name) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const greeting = yield* ctx.run(record(`hello ${name}`));
        const suffix = yield* ctx.run(record(`welcome ${name}`));
        return `${greeting}; ${suffix}`;
      }),
    durableSleep: (millis) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`sleeping ${millis}`));
        yield* ctx.sleep(Duration.millis(millis));
        return yield* ctx.run(record(`slept ${millis}`));
      }),
    sagaBooking: (trip) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`flight ${trip.tripId}`));
        yield* ctx.run(record(`hotel ${trip.tripId}`));
        if (trip.shouldFail) {
          yield* ctx.run(record(`cancel hotel ${trip.tripId}`));
          yield* ctx.run(record(`cancel flight ${trip.tripId}`));
          return `compensated ${trip.tripId}`;
        }
        yield* ctx.run(record(`car ${trip.tripId}`));
        return `booked ${trip.tripId}`;
      }),
    fanOutFanIn: (event) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const email = yield* ctx.beginRun(record(`email ${event.email}`));
        const sms = yield* ctx.beginRun(record(`sms ${event.phone}`));
        const slack = yield* ctx.beginRun(record(`slack ${event.orderId}`));
        const push = yield* ctx.beginRun(record(`push ${event.orderId}`));
        return yield* ctx.all([email.await, sms.await, slack.await, push.await]);
      }),
    distributedMutex: (workers) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        for (const worker of workers) {
          results.push(yield* ctx.run(record(`critical ${worker}`)));
        }
        return results;
      }),
    batchProcessor: ({ records, batchSize }) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const batches: Array<unknown> = [];
        for (let index = 0; index < records.length; index = index + batchSize) {
          batches.push(yield* ctx.run(record(`batch ${records.slice(index, index + batchSize).join(",")}`)));
        }
        return batches;
      }),
    priorityQueue: ({ jobs }) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const sorted = [...jobs].sort((left, right) => right.priority - left.priority);
        return yield* ctx.all(sorted.map((job) => ctx.run(record(`job ${job.id}`))));
      }),
    rateLimiter: (requests) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        for (const request of requests) {
          results.push(yield* ctx.run(record(`api ${request}`)));
          yield* ctx.sleep(Duration.millis(1));
        }
        return results;
      }),
    stateMachine: ({ orderId, path }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`created ${orderId}`));
        yield* ctx.run(record(`confirmed ${orderId}`));
        if (path === "cancel") {
          yield* ctx.run(record(`refunded ${orderId}`));
          return `cancelled ${orderId}`;
        }
        yield* ctx.run(record(`delivered ${orderId}`));
        return `delivered ${orderId}`;
      }),
    foodDelivery: ({ orderId, hasDriver }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`prepared ${orderId}`));
        if (!hasDriver) {
          yield* ctx.run(record(`refunded ${orderId}`));
          return `refunded ${orderId}`;
        }
        yield* ctx.run(record(`delivered ${orderId}`));
        return `complete ${orderId}`;
      }),
    eventSourcing: ({ userId, events }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        let projection = userId;
        for (const event of events) {
          projection = String(yield* ctx.run(record(`${projection}:${event}`)));
        }
        return projection;
      }),
    sessionLifecycle: ({ sessionId, userId, activities }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`login ${sessionId}:${userId}`));
        for (const activity of activities) {
          yield* ctx.run(record(`activity ${activity}`));
        }
        yield* ctx.sleep(Duration.millis(1));
        return yield* ctx.run(record(`expired ${sessionId}`));
      }),
    asyncHttpApi: (id) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        return yield* ctx.run(record(`async ${id}`));
      }),
    scheduleReport: (userId) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        return yield* ctx.run(record(`report ${userId}`));
      }),
    recursiveFactorial: (n) =>
      Effect.gen(function* (): Effect.fn.Return<number, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        if (n <= 1) {
          return 1;
        }
        const next = yield* ctx.rpc(recursiveFactorial, [n - 1], { target: group });
        return n * Number(next);
      }),
    loadBalancedCompute: (cost) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        return yield* ctx.run(record(`computed ${cost}`));
      }),
    webhookPayment: ({ eventId, amount }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        yield* ctx.run(record(`validated ${eventId}`));
        yield* ctx.run(record(`charged ${amount}`));
        return yield* ctx.run(record(`receipt ${eventId}`));
      }),
    healthMonitor: ({ services, iterations }) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const results: Array<unknown> = [];
        for (let index = 0; index < iterations; index = index + 1) {
          results.push(yield* ctx.run(record(`health ${index}:${services.join(",")}`)));
          yield* ctx.sleep(Duration.millis(1));
        }
        return results;
      }),
    agentOrchestration: ({ topic }) =>
      Effect.gen(function* (): Effect.fn.Return<unknown, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const research = yield* ctx.run(record(`research ${topic}`));
        const draft = yield* ctx.run(record(`write ${research}`));
        return yield* ctx.run(record(`review ${draft}`));
      }),
    imagePipeline: ({ prompt }) =>
      Effect.gen(function* (): Effect.fn.Return<ReadonlyArray<unknown>, unknown, ResonateContext.ResonateContext> {
        const ctx = yield* ResonateContext.ResonateContext;
        const photo = yield* ctx.beginRun(record(`photorealistic ${prompt}`));
        const cartoon = yield* ctx.beginRun(record(`cartoon ${prompt}`));
        const abstract = yield* ctx.beginRun(record(`abstract ${prompt}`));
        return yield* ctx.all([photo.await, cartoon.await, abstract.await]);
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
