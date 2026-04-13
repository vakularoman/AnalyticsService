using AnalyticsService;
using AnalyticsService.Services;
using AnalyticsService.Services.Background;
using AnalyticsService.Services.ClickHouse;
using Serilog;
using Serilog.Sinks.Grafana.Loki;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, config) =>
{
    var lokiUri      = context.Configuration["Loki:Uri"];
    var lokiLogin    = context.Configuration["Loki:Login"];
    var lokiPassword = context.Configuration["Loki:Password"];

    LokiCredentials? credentials = lokiLogin is not null && lokiPassword is not null
        ? new LokiCredentials { Login = lokiLogin, Password = lokiPassword }
        : null;

    config
        .ReadFrom.Configuration(context.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithMachineName()
        .Enrich.WithThreadId()
        .WriteTo.Console()
        .WriteTo.GrafanaLoki(
            lokiUri ?? "http://loki:3100",
            credentials: credentials,
            labels:
            [
                new LokiLabel { Key = "app", Value = "analytics" },
                new LokiLabel { Key = "env", Value = context.HostingEnvironment.EnvironmentName }
            ]
        );
});

builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.NumberHandling =
            System.Text.Json.Serialization.JsonNumberHandling.AllowNamedFloatingPointLiterals;
    });

builder.Services.AddSingleton<ClickHouseConnectionFactory>();
builder.Services.AddSingleton<ClickHouseSchemaInitializer>();
builder.Services.AddSingleton<AnalyticsEventService>();
builder.Services.AddSingleton<IBackgroundEventQueue, BackgroundEventQueue>();
builder.Services.AddHostedService<EventBatchHostedService>();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.UseMiddleware<ErrorHandlingMiddleware>();
app.UseSerilogRequestLogging();

app.UseAuthorization();
app.MapControllers();
app.MapHealthChecks("/health");

await InitializeDatabaseAsync(app);

app.Logger.LogInformation("AnalyticsService started. Environment: {Environment}", app.Environment.EnvironmentName);

app.Run();


static async Task InitializeDatabaseAsync(WebApplication app)
{
    using var scope = app.Services.CreateScope();
    var initializer = scope.ServiceProvider
        .GetRequiredService<ClickHouseSchemaInitializer>();

    await initializer.InitializeAsync();
}
