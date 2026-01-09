WITH sessions AS (
    SELECT
        vsl_name,
        buyer_name,
        MINIf(date, event_type = 'enter') AS enter_time,
        MINIf(date, event_type = 'play')  AS play_time,
        MAXIf(date, event_type = 'exit')  AS exit_time,
        MAX(video_length)                 AS video_length
    FROM analytics.events
    GROUP BY session_id, vsl_name, buyer_name
),

     filtered AS (
         SELECT
             *,
             play_time <= exit_time
                 AND play_time != toDateTime('1970-01-01')
                 AND exit_time != toDateTime('1970-01-01') AS is_video_watched,
             toUInt32(least(exit_time - play_time, video_length)) AS view_duration
         FROM sessions
         WHERE vsl_name = @VslName
           AND enter_time BETWEEN @StartDate AND @EndDate
     ),

     retention AS (
         SELECT
             arrayJoin(range(0, view_duration + 1)) AS elapsed_second
         FROM filtered
         WHERE is_video_watched
     )

SELECT
    countIf(is_video_watched) / count() AS ViewRate,
    avgIf(view_duration, is_video_watched)  AS AvgViewDuration,
    count()  AS TotalSiteViews,
    countIf(is_video_watched)   AS TotalVideoViews,
    countIf(is_video_watched AND view_duration >= video_length)
        / countIf(is_video_watched) AS CompletionRate,
    (
        SELECT groupArray(cnt)
        FROM (
                 SELECT
                     count() AS cnt
                 FROM retention
                 GROUP BY elapsed_second
                 ORDER BY elapsed_second
                 )
    )   AS RetentionViewers
FROM filtered;
