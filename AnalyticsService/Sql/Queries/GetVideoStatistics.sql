WITH filtered AS (
    SELECT *,
           toUInt32(exit_time - play_time) AS view_duration
    FROM analytics.video_sessions_mv
    WHERE vsl_name = @VslName
      AND play_time BETWEEN @StartDate AND @EndDate
      AND is_valid = 1
),
retention_expanded AS (
    SELECT arrayJoin(range(0, view_duration + 1)) AS elapsed_second
    FROM (SELECT view_duration FROM filtered))
SELECT
    (SELECT COUNT(*) FROM filtered) AS TotalSessions,
    (SELECT COUNTIf(view_duration >= video_length) FROM filtered) AS CompletedSessions,
    (SELECT COUNTIf(view_duration >= video_length) / COUNT(*) FROM filtered) AS CompletionRate,
    (SELECT AVG(view_duration) FROM filtered) AS AvgViewDuration,
    groupArray(count) AS RetentionViewers
FROM (SELECT COUNT(*) AS count
      FROM retention_expanded
      GROUP BY elapsed_second
      ORDER BY elapsed_second)