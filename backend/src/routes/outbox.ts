import { Router } from "express";
import { getOutboxCollection } from "../utils/mongodb.js";

type StatusCount = {
  _id: string;
  count: number;
};

export const outboxRouter = Router();

outboxRouter.get("/outbox/status", async (_request, response) => {
  try {
    const collection = getOutboxCollection();

    const [statusCounts, oldestPending, recentFailures] = await Promise.all([
      collection
        .aggregate<StatusCount>([
          {
            $group: {
              _id: "$status",
              count: { $sum: 1 }
            }
          }
        ])
        .toArray(),

      collection.findOne(
        {
          status: "pending"
        },
        {
          sort: {
            nextAttemptAt: 1
          },
          projection: {
            _id: 0,
            eventId: 1,
            attempts: 1,
            createdAt: 1,
            nextAttemptAt: 1,
            lastError: 1
          }
        }
      ),

      collection
        .find(
          {
            lastError: {
              $exists: true
            }
          },
          {
            projection: {
              _id: 0,
              eventId: 1,
              status: 1,
              attempts: 1,
              createdAt: 1,
              updatedAt: 1,
              nextAttemptAt: 1,
              lastError: 1
            }
          }
        )
        .sort({
          updatedAt: -1
        })
        .limit(5)
        .toArray()
    ]);

    const counts = {
      pending: 0,
      processing: 0,
      published: 0
    };

    for (const item of statusCounts) {
      if (item._id === "pending") {
        counts.pending = item.count;
      }

      if (item._id === "processing") {
        counts.processing = item.count;
      }

      if (item._id === "published") {
        counts.published = item.count;
      }
    }

    response.json({
      status: "ok",
      generatedAt: new Date().toISOString(),
      counts,
      oldestPending,
      recentFailures
    });
  } catch (error) {
    console.error("Failed to retrieve outbox status:", error);

    response.status(500).json({
      status: "error",
      message: "Failed to retrieve outbox status"
    });
  }
});
