using AnalyticsService.Models;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using Dapper;
using ClickHouseConnectionFactory = AnalyticsService.Services.ClickHouse.ClickHouseConnectionFactory;

namespace AnalyticsService.Services;

public sealed class AnalyticsEventService
{
    private readonly ClickHouseConnectionFactory _factory;

    private readonly string _getVideoStatisticsSql;
    private readonly string _getVslBuyBuyerNameSql;

    public AnalyticsEventService(ClickHouseConnectionFactory factory, IWebHostEnvironment env)
    {
        _factory = factory;

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

    public async Task InsertEventsBatchAsync(IEnumerable<AnalyticsEvent> events)
    {
        await using var connection = _factory.Create();
        await connection.OpenAsync();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "analytics.events",
            BatchSize = 1_000,
        };

        await bulkCopy.InitAsync();
        var rows = events.Select(e => new object[]
        {
            e.Date,
            e.EventType,
            e.VslName,
            e.BuyerName,
            e.FbId,
            e.SessionId,
            e.ClickId,
            e.VideoLength,
        });
        await bulkCopy.WriteToServerAsync(rows);
    }

    public async Task<VideoStatistics?> GetVideoStatistics(string vslName, DateTime startDate, DateTime endDate)
    {
        var queryResult = await QueryAsync<VideoStatistics>(_getVideoStatisticsSql,
            new { VslName = vslName, StartDate = startDate, EndDate = endDate });
        
        var statistics = queryResult.FirstOrDefault();
        statistics?.CalculateGraphsStatistics();

        return statistics;
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
