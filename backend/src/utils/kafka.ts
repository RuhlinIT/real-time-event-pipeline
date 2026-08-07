import {
  Kafka,
  Partitioners,
  Producer
} from "kafkajs";
import { config } from "./config.js";

const kafka = new Kafka({
  clientId: config.KAFKA_CLIENT_ID,
  brokers: [config.KAFKA_BROKER]
});

const producer: Producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner
});

export async function connectKafka() {
  await producer.connect();
  console.log("Connected to Kafka");
}

export async function publishEvent(
  event: Record<string, unknown>
) {
  await producer.send({
    topic: config.KAFKA_TOPIC,
    messages: [
      {
        key: String(event.userId),
        value: JSON.stringify(event)
      }
    ]
  });
}

export async function closeKafka() {
  await producer.disconnect();
  console.log("Kafka connection closed");
}
