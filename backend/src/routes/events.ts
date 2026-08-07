import { Router } from "express";
import { randomUUID } from "node:crypto";
import type { Document } from "mongodb";
import { trackingEventSchema } from "../utils/event-schema.js";
import {
  getEventsCollection,
  getMongoClient,
  getOutboxCollection
} from "../utils/mongodb.js";
import { createOutboxMessage } from "../utils/outbox.js";

export const eventsRouter = Router();

eventsRouter.post("/track", async (request, response) => {
  const parsed = trackingEventSchema.safeParse(request.body);

  if (!parsed.success) {
    response.status(400).json({
      status: "error",
      message: "Invalid event payload",
      errors: parsed.error.flatten()
    });

    return;
  }

  const input = parsed.data;
  const eventId = randomUUID();
  const eventTimestamp = input.timestamp
    ? new Date(input.timestamp)
    : new Date();
  const receivedAt = new Date();

  const event = {
    eventId,
    eventType: input.eventType,
    userId: input.userId,
    sessionId: input.sessionId,
    eventTimestamp,
    receivedAt,
    properties: input.properties
  };

  const kafkaPayload = {
    eventId,
    eventType: input.eventType,
    userId: input.userId,
    sessionId: input.sessionId,
    eventTimestamp: eventTimestamp.toISOString(),
    receivedAt: receivedAt.toISOString(),
    properties: JSON.stringify(input.properties)
  };

  const outboxMessage = createOutboxMessage({
    eventId,
    payload: kafkaPayload
  });

  const session = getMongoClient().startSession();

  try {
    let outboxId = "";

    await session.withTransaction(async () => {
      await getEventsCollection().insertOne(event, { session });

      const outboxResult = await getOutboxCollection().insertOne(
        outboxMessage,
        { session }
      );

      outboxId = outboxResult.insertedId.toHexString();
    });

    response.status(202).json({
      status: "accepted",
      eventId,
      outboxId
    });
  } catch (error) {
    console.error("Transaction failed:", error);

    response.status(500).json({
      status: "error",
      message: "Event transaction failed"
    });
  } finally {
    await session.endSession();
  }
});
