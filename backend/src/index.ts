import { app } from "./server.js";
import { config } from "./utils/config.js";
import { connectKafka, closeKafka } from "./utils/kafka.js";
import { connectMongo, closeMongo } from "./utils/mongodb.js";
import { startOutboxRelay, stopOutboxRelay } from "./utils/outbox.js";

let httpServer: ReturnType<typeof app.listen>;

async function start() {
  await connectMongo();
  await connectKafka();
  startOutboxRelay();

  httpServer = app.listen(config.PORT, () => {
    console.log(`Backend listening on http://localhost:${config.PORT}`);
    console.log(`Health check: http://localhost:${config.PORT}/api/health`);
    console.log(`Event endpoint: http://localhost:${config.PORT}/api/track`);
  });
}

async function shutdown(signal: string) {
  console.log(`${signal} received. Shutting down...`);

  await stopOutboxRelay();

  httpServer?.close(async () => {
    await closeKafka();
    await closeMongo();
    process.exit(0);
  });
}

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

start().catch((error) => {
  console.error("Backend startup failed:", error);
  process.exit(1);
});