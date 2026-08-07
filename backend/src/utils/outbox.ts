import { getOutboxCollection } from "./mongodb.js";
import { publishEvent } from "./kafka.js";
import { ObjectId } from "mongodb";

type OutboxMessage = {
  _id: ObjectId;
  eventId: string;
  payload: Record<string, unknown>;
  status: "pending" | "processing" | "published";
  attempts: number;
  nextAttemptAt: Date;
  lockedAt?: Date;
};

const POLL_INTERVAL_MS = 1000;
const LOCK_TIMEOUT_MS = 60_000;
const MAX_BACKOFF_MS = 60_000;

let running = false;
let relayPromise: Promise<void> | undefined;

function retryDelay(attempts: number) {
  return Math.min(
    1000 * 2 ** Math.min(attempts, 6),
    MAX_BACKOFF_MS
  );
}

async function claimNextMessage(): Promise<OutboxMessage | null> {
  const collection = getOutboxCollection();
  const now = new Date();
  const expiredLock = new Date(Date.now() - LOCK_TIMEOUT_MS);

  const message = await collection.findOneAndUpdate(
    {
      $or: [
        {
          status: "pending",
          nextAttemptAt: { $lte: now }
        },
        {
          status: "processing",
          lockedAt: { $lte: expiredLock }
        }
      ]
    },
    {
      $set: {
        status: "processing",
        lockedAt: now,
        updatedAt: now
      },
      $inc: {
        attempts: 1
      }
    },
    {
      sort: {
        nextAttemptAt: 1
      },
      returnDocument: "after"
    }
  );

  return message as OutboxMessage | null;
}

async function processMessage(message: OutboxMessage) {
  const collection = getOutboxCollection();

  try {
    await publishEvent(message.payload);

    await collection.updateOne(
      {
        _id: message._id as any,
        status: "processing"
      },
      {
        $set: {
          status: "published",
          publishedAt: new Date(),
          updatedAt: new Date()
        },
        $unset: {
          lockedAt: "",
          lastError: ""
        }
      }
    );

    console.log(`Published outbox event ${message.eventId}`);
  } catch (error) {
    const delay = retryDelay(message.attempts);
    const nextAttemptAt = new Date(Date.now() + delay);
    const lastError =
      error instanceof Error ? error.message : String(error);

    await collection.updateOne(
      {
        _id: message._id as any,
        status: "processing"
      },
      {
        $set: {
          status: "pending",
          nextAttemptAt,
          lastError,
          updatedAt: new Date()
        },
        $unset: {
          lockedAt: ""
        }
      }
    );

    console.error(
      `Outbox event ${message.eventId} failed; retrying in ${delay}ms`,
      lastError
    );
  }
}

async function relayLoop() {
  while (running) {
    const message = await claimNextMessage();

    if (message) {
      await processMessage(message);
    } else {
      await new Promise((resolve) =>
        setTimeout(resolve, POLL_INTERVAL_MS)
      );
    }
  }
}

export function createOutboxMessage(input: {
  eventId: string;
  payload: Record<string, unknown>;
}) {
  const now = new Date();

  return {
    eventId: input.eventId,
    payload: input.payload,
    status: "pending" as const,
    attempts: 0,
    nextAttemptAt: now,
    createdAt: now,
    updatedAt: now
  };
}

export function startOutboxRelay() {
  if (running) {
    return;
  }

  running = true;
  relayPromise = relayLoop();

  console.log("Outbox relay started");
}

export async function stopOutboxRelay() {
  running = false;

  if (relayPromise) {
    await relayPromise;
    relayPromise = undefined;
  }

  console.log("Outbox relay stopped");
}
