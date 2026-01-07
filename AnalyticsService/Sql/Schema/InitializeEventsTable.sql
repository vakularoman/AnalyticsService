CREATE TABLE IF NOT EXISTS analytics.events
(
    date        DateTime,
    event_type  LowCardinality(String),
    vsl_name    LowCardinality(String),
    buyer_name  LowCardinality(String),
    fb_id       String,
    session_id  String,
    click_id    String,
    video_length  UInt32
)
    ENGINE = MergeTree
PARTITION BY toDate(date)
ORDER BY (vsl_name, date, event_type)
TTL date + INTERVAL 1 YEAR;