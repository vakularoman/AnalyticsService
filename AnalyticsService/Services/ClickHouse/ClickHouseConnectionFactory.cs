using ClickHouse.Driver.ADO;

namespace AnalyticsService.Services.ClickHouse;

public sealed class ClickHouseConnectionFactory
{
    private readonly string _cs;

    public ClickHouseConnectionFactory(IConfiguration cfg)
        => _cs = cfg["ClickHouse:ConnectionString"]!;

    public ClickHouseConnection Create()
        => new ClickHouseConnection(_cs);
}