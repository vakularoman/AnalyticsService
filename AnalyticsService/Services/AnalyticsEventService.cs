using AnalyticsService.Models;
using AnalyticsService.Services.ClickHouse;
using Dapper;

namespace AnalyticsService.Services;

public sealed class AnalyticsEventService
{
    private readonly ClickHouseConnectionFactory _factory;

    private readonly string _insertSql;
    private readonly string _getVideoStatisticsSql;

    public AnalyticsEventService(ClickHouseConnectionFactory factory, IWebHostEnvironment env)
    {
        _factory = factory;

        var insertFile = Path.Combine(env.ContentRootPath, "Sql/Queries/InsertEvent.sql");
        _insertSql = File.ReadAllText(insertFile);

        var getVideoStatistics = Path.Combine(env.ContentRootPath, "Sql/Queries/GetVideoStatistics.sql");
        _getVideoStatisticsSql = File.ReadAllText(getVideoStatistics);
    }

    public async Task InsertEventsBatchAsync(IEnumerable<AnalyticsEvent> events)
    {
        var eventsList = events.ToList();
        if (eventsList.Count == 0)
        {
            return;
        }

        await using var conn = _factory.Create();
        await conn.OpenAsync();

        await conn.ExecuteAsync(_insertSql, eventsList);
    }

    public async Task<VideoStatistics?> GetVideoStatistics(string vslName, DateTime startDate, DateTime endDate)
    {
        await using var conn = _factory.Create();
        await conn.OpenAsync();

        var result = await conn.QueryAsync<VideoStatistics>(_getVideoStatisticsSql,
            new { VslName = vslName, StartDate = startDate, EndDate = endDate });
        return result.FirstOrDefault();
    }
}