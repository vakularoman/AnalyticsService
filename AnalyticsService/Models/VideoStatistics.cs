namespace AnalyticsService.Models;

public sealed class VideoStatistics
{
    public long TotalSessions { get; init; }

    public long CompletedSessions { get; init; }

    public double CompletionRate { get; init; }

    public double AvgViewDuration { get; init; }

    public IReadOnlyList<long> RetentionViewers { get; init; } = Array.Empty<long>();
}