using AnalyticsService.Services;
using AnalyticsService.Services.Background;
using AnalyticsService.Services.ClickHouse;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.NumberHandling = 
            System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals;
    });

// Add services to the container.
builder.Services.AddSingleton<ClickHouseConnectionFactory>();
builder.Services.AddSingleton<ClickHouseSchemaInitializer>();
builder.Services.AddSingleton<AnalyticsEventService>();
builder.Services.AddSingleton<IBackgroundEventQueue, BackgroundEventQueue>();
builder.Services.AddHostedService<EventBatchHostedService>();
builder.Services.AddControllers();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");
//app.Urls.Add("http://0.0.0.0:80");
// Configure the HTTP request pipeline.
app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

await InitializeDatabaseAsync(app);

app.Run();


static async Task InitializeDatabaseAsync(WebApplication app)
{
    using var scope = app.Services.CreateScope();
    var initializer = scope.ServiceProvider
        .GetRequiredService<ClickHouseSchemaInitializer>();

    await initializer.InitializeAsync();
}