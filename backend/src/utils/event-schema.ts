import { z } from "zod";

export const trackingEventSchema = z.object({
  eventType: z.string().min(1),
  userId: z.string().min(1),
  sessionId: z.string().min(1),
  timestamp: z.string().datetime({ offset: true }).optional(),
  properties: z
    .record(z.string(), z.unknown())
    .default({})
});

export type TrackingEventInput = z.infer<typeof trackingEventSchema>;
