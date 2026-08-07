CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_analytics
(
    eventId String,
    eventType LowCardinality(String),
    userId String,
    sessionId String,
    eventTimestamp DateTime64(3, 'UTC'),
    receivedAt DateTime64(3, 'UTC'),
    properties String,
    eventDate Date MATERIALIZED toDate(eventTimestamp),
    eventHour DateTime MATERIALIZED toStartOfHour(eventTimestamp)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(eventTimestamp)
ORDER BY (eventType, eventTimestamp, userId, eventId);

CREATE TABLE IF NOT EXISTS analytics.events_kafka
(
    eventId String,
    eventType String,
    userId String,
    sessionId String,

    -- Kafka JSON arrives as ISO strings, e.g. 2026-08-06T23:43:05.655Z
    eventTimestamp String,
    receivedAt String,

    properties String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'user-events',
    kafka_group_name = 'clickhouse-events-consumer-v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.events_kafka_mv
TO analytics.events_analytics
AS
SELECT
    eventId,
    eventType,
    userId,
    sessionId,
    parseDateTime64BestEffort(eventTimestamp, 3, 'UTC') AS eventTimestamp,
    parseDateTime64BestEffort(receivedAt, 3, 'UTC') AS receivedAt,
    properties
FROM analytics.events_kafka;
