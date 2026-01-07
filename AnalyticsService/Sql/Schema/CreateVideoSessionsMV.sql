CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.video_sessions_mv
ENGINE = MergeTree
PARTITION BY toDate(play_time)
ORDER BY (vsl_name, play_time)
AS
SELECT
    vsl_name,
    MIN(date) FILTER (WHERE event_type='play') AS play_time,
    MAX(date) FILTER (WHERE event_type='exit') AS exit_time,
    MAX(video_length) AS video_length,
    toUInt32(MAX(date) FILTER (WHERE event_type='exit') - MIN(date) FILTER (WHERE event_type='play')) AS view_duration,
    (MIN(date) FILTER (WHERE event_type='play') IS NOT NULL
        AND MAX(date) FILTER (WHERE event_type='exit') IS NOT NULL) AS is_valid,
    buyer_name
FROM analytics.events
GROUP BY session_id, vsl_name, buyer_name;