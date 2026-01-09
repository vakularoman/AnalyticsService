WITH sessions AS (
    SELECT
        MINIf(date, event_type = 'enter') AS enter_time,
        MINIf(date, event_type = 'play')  AS play_time,
        MAXIf(date, event_type = 'exit')  AS exit_time,
        any(video_length)                 AS video_length
    FROM analytics.events
    WHERE vsl_name = @VslName AND date >= @StartDate
    GROUP BY session_id),

     filtered AS (
         SELECT
             video_length,
             play_time <= exit_time
                 AND play_time != toDateTime('1970-01-01')
                 AND exit_time != toDateTime('1970-01-01') AS is_video_watched,
             toUInt32(least(exit_time - play_time, video_length)) AS view_duration
         FROM sessions
         WHERE enter_time <= @EndDate)

SELECT
    countIf(is_video_watched) / count() AS ViewRate,
    avgIf(view_duration, is_video_watched)  AS AvgViewDuration,
    count()  AS TotalSiteViews,
    countIf(is_video_watched)   AS TotalVideoViews,
    countIf(is_video_watched AND view_duration >= video_length)
        / countIf(is_video_watched) AS CompletionRate,
    groupArray(view_duration) as ViewDuration,
    any(video_length) as VideoLength
FROM filtered;
