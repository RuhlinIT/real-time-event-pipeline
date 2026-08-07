import express from "express";
import cors from "cors";
import { config } from "./utils/config.js";
import { eventsRouter } from "./routes/events.js";
import { outboxRouter } from "./routes/outbox.js";
import { adminRouter } from "./routes/admin.js";

export const app = express();

app.use(
  cors({
    origin: config.FRONTEND_ORIGIN
  })
);

app.use(express.json({ limit: "100kb" }));

app.get("/api/health", (_request, response) => {
  response.json({
    status: "ok",
    service: "backend",
    timestamp: new Date().toISOString()
  });
});

app.use("/api", eventsRouter);
app.use("/api", outboxRouter);
app.use("/api", adminRouter);