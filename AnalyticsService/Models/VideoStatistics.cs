using System.Text.Json.Serialization;

namespace AnalyticsService.Models;

public sealed class VideoStatistics
{
    public double ViewRate { get; init; }

    public double AvgViewDuration { get; init; }

    public long TotalSiteViews { get; init; }

    public long TotalVideoViews { get; init; }

    public double CompletionRate { get; init; }

    [JsonIgnore]
    public IReadOnlyList<uint> ViewDuration { get; init; } = Array.Empty<uint>();

    [JsonIgnore]
    public long VideoLength { get; init; }

    public IReadOnlyList<long> RetentionViewers { get; set; } = Array.Empty<long>();

    public void CalculateGraphsStatistics()
    {
        var counts = new long[VideoLength + 1];
        foreach (var d in ViewDuration) counts[d]++;

        long[] retention = new long[counts.Length];
        long sum = 0;
        for (int i = counts.Length - 1; i >= 0; i--)
        {
            sum += counts[i];
            retention[i] = sum;
        }

        RetentionViewers = retention;
    }
}