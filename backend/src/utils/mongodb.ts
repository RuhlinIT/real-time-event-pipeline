import {
  Collection,
  Db,
  Document,
  MongoClient
} from "mongodb";
import { config } from "./config.js";

const client = new MongoClient(config.MONGODB_URI);

let database: Db;
let eventsCollection: Collection<Document>;
let outboxCollection: Collection<Document>;

export async function connectMongo() {
  await client.connect();

  database = client.db(config.MONGODB_DATABASE);
  eventsCollection = database.collection(config.MONGODB_COLLECTION);
  outboxCollection = database.collection("event_outbox");

  await eventsCollection.createIndex(
    { eventId: 1 },
    { unique: true }
  );

  await eventsCollection.createIndex({
    userId: 1,
    receivedAt: -1
  });

  await outboxCollection.createIndex(
    { eventId: 1 },
    { unique: true }
  );

  await outboxCollection.createIndex({
    status: 1,
    nextAttemptAt: 1
  });

  console.log("Connected to MongoDB");
}

export function getMongoClient() {
  return client;
}

export function getEventsCollection() {
  return eventsCollection;
}

export function getOutboxCollection() {
  return outboxCollection;
}

export async function closeMongo() {
  await client.close();
  console.log("MongoDB connection closed");
}
