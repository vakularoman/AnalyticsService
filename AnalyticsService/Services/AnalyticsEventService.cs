using AnalyticsService.Models;
using ClickHouse.Driver.ADO;
using Dapper;
using ClickHouseConnectionFactory = AnalyticsService.Services.ClickHouse.ClickHouseConnectionFactory;

namespace AnalyticsService.Services;

public sealed class AnalyticsEventService
{
    private readonly ClickHouseConnectionFactory _factory;

    private readonly string _insertSql;
    private readonly string _getVideoStatisticsSql;
    private readonly string _getVslBuyBuyerNameSql;

    public AnalyticsEventService(ClickHouseConnectionFactory factory, IWebHostEnvironment env)
    {
        _factory = factory;

        _insertSql = LoadSql(env, "InsertEvent.sql");
        _getVideoStatisticsSql = LoadSql(env, "GetVideoStatistics.sql");
        _getVslBuyBuyerNameSql = LoadSql(env, "GetVslByBuyerName.sql");
    }

    private static string LoadSql(IWebHostEnvironment env, string fileName)
    {
        var path = Path.Combine(env.ContentRootPath, "Sql/Queries", fileName);
        return File.ReadAllText(path);
    }

    private async Task<T> ExecuteWithConnectionAsync<T>(Func<ClickHouseConnection, Task<T>> func)
    {
        await using var conn = _factory.Create();
        await conn.OpenAsync();
        return await func(conn);
    }

    private Task<IReadOnlyList<T>> QueryAsync<T>(string sql, object? parameters = null)
        => ExecuteWithConnectionAsync(async conn => (await conn.QueryAsync<T>(sql, parameters)).ToList() as IReadOnlyList<T>);

    private Task<int> ExecuteAsync(string sql, object? parameters = null)
        => ExecuteWithConnectionAsync(conn => conn.ExecuteAsync(sql, parameters));

    public Task InsertEventsBatchAsync(IEnumerable<AnalyticsEvent> events)
    {
        var eventsList = events.ToList();
        if (!eventsList.Any())
        {
            return Task.CompletedTask;
        }

        return ExecuteAsync(_insertSql, eventsList);
    }

    public async Task<VideoStatistics?> GetVideoStatistics(string vslName, DateTime startDate, DateTime endDate)
    {
        var result = await QueryAsync<VideoStatistics>(_getVideoStatisticsSql,
            new { VslName = vslName, StartDate = startDate, EndDate = endDate });

        return result.FirstOrDefault();
    }

    public async Task<ICollection<string>> GetVlsByBuyerName(ICollection<string> buyerNames)
    {
        if (!buyerNames.Any())
        {
            return Array.Empty<string>();
        }

        var result = await QueryAsync<string>(_getVslBuyBuyerNameSql, new { BuyerNames = buyerNames });
        return result.ToList();
    }
}
