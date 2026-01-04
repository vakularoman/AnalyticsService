SELECT
    timestamp,
    event_type AS EventType,
    user_id AS UserId,
    session_id AS SessionId,
    element_id AS ElementId,
    element_class AS ElementClass,
    duration_seconds AS DurationSeconds,
    form_name AS FormName
FROM analytics.events
ORDER BY timestamp DESC
    LIMIT @Count