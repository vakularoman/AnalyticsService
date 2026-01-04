CREATE TABLE IF NOT EXISTS analytics.events
(
    timestamp DateTime64(3) CODEC(Delta, LZ4),
    event_type LowCardinality(String),
    user_id UInt64,
    session_id UUID,
    element_id Nullable(String),
    element_class Nullable(String),
    duration_seconds Nullable(UInt32),
    form_name Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, session_id, event_type)
TTL timestamp + INTERVAL 2 YEAR;