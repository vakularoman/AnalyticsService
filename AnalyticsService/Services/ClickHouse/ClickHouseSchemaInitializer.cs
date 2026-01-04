using Dapper;

namespace AnalyticsService.Services.ClickHouse;

public sealed class ClickHouseSchemaInitializer
{
    private readonly ClickHouseConnectionFactory _factory;
    private readonly IWebHostEnvironment _env;

    public ClickHouseSchemaInitializer(
        ClickHouseConnectionFactory factory,
        IWebHostEnvironment env)
    {
        _factory = factory;
        _env = env;
    }

    public async Task InitializeAsync()
    {
        await using var conn = _factory.Create();
        await conn.OpenAsync();

        var sqlFiles = new[]
        {
            Path.Combine(_env.ContentRootPath, "Sql/Schema/InitializeEventsTable.sql")
        };

        foreach (var file in sqlFiles)
        {
            var sql = await File.ReadAllTextAsync(file);
            if (!string.IsNullOrWhiteSpace(sql))
            {
                await conn.ExecuteAsync(sql);
            }
        }
    }
}
