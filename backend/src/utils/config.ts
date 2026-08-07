import "dotenv/config";
import { z } from "zod";

const configSchema = z.object({
  NODE_ENV: z
    .enum(["development", "test", "production"])
    .default("development"),

  PORT: z.coerce.number().default(3001),

  FRONTEND_ORIGIN: z
    .string()
    .default("http://localhost:5173"),

  KAFKA_BROKER: z
    .string()
    .default("localhost:9092"),

  KAFKA_TOPIC: z
    .string()
    .default("user-events"),

  KAFKA_CLIENT_ID: z
    .string()
    .default("event-pipeline-api"),

  MONGODB_URI: z
    .string()
    .min(1),

  MONGODB_DATABASE: z
    .string()
    .default("events"),

  MONGODB_COLLECTION: z
    .string()
    .default("raw_events"),

  ADMIN_API_KEY: z.string().min(16).optional(),
});

const parsedConfig = configSchema.safeParse(process.env);

if (!parsedConfig.success) {
  console.error("Invalid backend environment configuration:");
  console.error(parsedConfig.error.flatten().fieldErrors);
  process.exit(1);
}

export const config = parsedConfig.data;
