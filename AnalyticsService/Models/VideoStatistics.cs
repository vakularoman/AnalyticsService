namespace AnalyticsService.Models;

public sealed class VideoStatistics
{
    public double ViewRate { get; init; }

    public double AvgViewDuration { get; init; }

    public long TotalSiteViews { get; init; }

    public long TotalVideoViews { get; init; }

    public double CompletionRate { get; init; }

    public IReadOnlyList<long> RetentionViewers { get; init; } = Array.Empty<long>();
}