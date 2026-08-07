import { Router } from "express";
import { z } from "zod";

import { config } from "../utils/config.js";
import { getOutboxCollection } from "../utils/mongodb.js";

const eventIdSchema = z.string().uuid();

const requeueBodySchema = z.object({
  reason: z.string().trim().min(1).max(250).optional()
});

export const adminRouter = Router();

adminRouter.use((request, response, next) => {
  if (!config.ADMIN_API_KEY) {
    response.status(503).json({
      status: "error",
      message: "Admin API is disabled"
    });

    return;
  }

  const adminKey = request.header("x-admin-key");

  if (!adminKey || adminKey !== config.ADMIN_API_KEY) {
    response.status(401).json({
      status: "error",
      message: "Invalid admin key"
    });

    return;
  }

  next();
});

adminRouter.post(
  "/admin/outbox/:eventId/requeue",
  async (request, response) => {
    const eventIdResult = eventIdSchema.safeParse(
      request.params.eventId
    );

    if (!eventIdResult.success) {
      response.status(400).json({
        status: "error",
        message: "eventId must be a UUID"
      });

      return;
    }

    const bodyResult = requeueBodySchema.safeParse(
      request.body ?? {}
    );

    if (!bodyResult.success) {
      response.status(400).json({
        status: "error",
        message: "Invalid requeue request",
        errors: bodyResult.error.flatten()
      });

      return;
    }

    const eventId = eventIdResult.data;
    const reason = bodyResult.data.reason ?? "Manual test requeue";
    const collection = getOutboxCollection();

    try {
      const existing = await collection.findOne(
        {
          eventId
        },
        {
          projection: {
            _id: 0,
            eventId: 1,
            status: 1
          }
        }
      );

      if (!existing) {
        response.status(404).json({
          status: "error",
          message: "Outbox event not found"
        });

        return;
      }

      if (existing.status === "processing") {
        response.status(409).json({
          status: "error",
          message:
            "Outbox event is currently processing; wait for the relay or retry later"
        });

        return;
      }

      const now = new Date();

      const updated = await collection.findOneAndUpdate(
        {
          eventId,
          status: {
            $ne: "processing"
          }
        },
        {
          $set: {
            status: "pending",
            nextAttemptAt: now,
            updatedAt: now,
            requeuedAt: now,
            requeueReason: reason
          },
          $unset: {
            lockedAt: "",
            lastError: "",
            publishedAt: ""
          },
          $inc: {
            manualRequeueCount: 1
          }
        },
        {
          returnDocument: "after",
          projection: {
            _id: 0,
            eventId: 1,
            status: 1,
            attempts: 1,
            nextAttemptAt: 1,
            manualRequeueCount: 1,
            requeuedAt: 1,
            requeueReason: 1
          }
        }
      );

      response.status(202).json({
        status: "requeued",
        outboxEvent: updated
      });
    } catch (error) {
      console.error("Failed to manually requeue outbox event:", error);

      response.status(500).json({
        status: "error",
        message: "Failed to requeue outbox event"
      });
    }
  }
);
