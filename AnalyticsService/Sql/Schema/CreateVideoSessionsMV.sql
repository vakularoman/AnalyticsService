CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.video_sessions_mv
ENGINE = MergeTree
PARTITION BY toDate(play_time)
ORDER BY (vsl_name, play_time)
AS
SELECT
    vsl_name,
    play_time,
    exit_time,
    enter_time,
    video_length,
    toUInt32(exit_time - play_time) AS view_duration,
    (play_time != toDateTime('1970-01-01') AND exit_time != toDateTime('1970-01-01')) AND play_time <= exit_time AS is_video_watched,
    buyer_name
    FROM (
         SELECT
             vsl_name,
             buyer_name,
             MIN(date) FILTER (WHERE event_type='enter') AS enter_time,
             MIN(date) FILTER (WHERE event_type='play') AS play_time,
             MAX(date) FILTER (WHERE event_type='exit') AS exit_time,
             MAX(video_length) AS video_length
         FROM analytics.events
         GROUP BY session_id, vsl_name, buyer_name
     ) AS t;
