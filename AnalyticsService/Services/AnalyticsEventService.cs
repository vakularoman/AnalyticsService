using AnalyticsService.DTO;
using AnalyticsService.Services.ClickHouse;
using Dapper;

namespace AnalyticsService.Services;

public sealed class AnalyticsEventService
{
    private readonly ClickHouseConnectionFactory _factory;
    private readonly string _insertSql;
    private readonly string _getLastEventsSql;

    public AnalyticsEventService(ClickHouseConnectionFactory factory, IWebHostEnvironment env)
    {
        _factory = factory;

        var insertFile = Path.Combine(env.ContentRootPath, "Sql/Queries/InsertEvent.sql");
        _insertSql = File.ReadAllText(insertFile);

        var getLastFile = Path.Combine(env.ContentRootPath, "Sql/Queries/GetLastEvents.sql");
        _getLastEventsSql = File.ReadAllText(getLastFile);
    }

    public async Task InsertEventsBatchAsync(IEnumerable<AnalyticsEventDto> events)
    {
        var eventsList = events.ToList();
        if (eventsList.Count == 0)
        {
            return;
        }

        var parameters = eventsList.Select(e => new
        {
            Timestamp = DateTime.UtcNow,
            e.EventType,
            e.UserId,
            e.SessionId,
            e.ElementId,
            e.ElementClass,
            e.DurationSeconds,
            e.FormName
        });

        await using var conn = _factory.Create();
        await conn.OpenAsync();

        await conn.ExecuteAsync(_insertSql, parameters);
    }

    public async Task<IReadOnlyList<AnalyticsEventDto>> GetLastEventsAsync(int count = 10)
    {
        await using var conn = _factory.Create();
        await conn.OpenAsync();

        var events = await conn.QueryAsync<AnalyticsEventDto>(_getLastEventsSql, new { Count = count });
        return events.ToList();
    }
}