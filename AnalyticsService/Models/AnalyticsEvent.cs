namespace AnalyticsService.Models;

public sealed class AnalyticsEvent
{
    public DateTime Date { get; init; }

    public string EventType { get; init; } = null!;

    public string? VslName { get; init; }

    public string BuyerName { get; init; } = null!;

    public string FbId { get; init; } = null!;

    public string SessionId { get; init; } = null!;

    public string? ClickId { get; init; }

    public uint? VideoLength { get; init; }
}