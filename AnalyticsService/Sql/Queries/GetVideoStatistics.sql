WITH filtered AS (
    SELECT *,
           toUInt32(least(exit_time - play_time, video_length)) AS view_duration
    FROM analytics.video_sessions_mv
    WHERE vsl_name = @VslName
      AND enter_time BETWEEN @StartDate AND @EndDate
),
retention_expanded AS (
    SELECT arrayJoin(range(0, view_duration + 1)) AS elapsed_second
    FROM (SELECT view_duration FROM filtered WHERE is_video_watched = 1))
SELECT
    (SELECT IF(count() = 0, 0, COUNTIf(is_video_watched = 1) / COUNT()) FROM filtered) AS ViewRate,
    (SELECT AVG(view_duration) FROM filtered WHERE is_video_watched = 1) AS AvgViewDuration,
    (SELECT COUNT(*) FROM filtered) AS TotalSiteViews,
    (SELECT COUNT(*) FROM filtered WHERE is_video_watched = 1) AS TotalVideoViews,
    (SELECT COUNTIf(is_video_watched AND view_duration >= video_length) / COUNTIf(is_video_watched) FROM filtered) AS CompletionRate,
    groupArray(count) AS RetentionViewers
FROM (SELECT COUNT(*) AS count
      FROM retention_expanded
      GROUP BY elapsed_second
      ORDER BY elapsed_second)