using System.Collections.Concurrent;
using AnalyticsService.Models;

namespace AnalyticsService.Services.Background;

public interface IBackgroundEventQueue
{
    void Enqueue(List<AnalyticsEvent> evt);

    void Enqueue(AnalyticsEvent evt);

    IReadOnlyList<AnalyticsEvent> DequeueAll();
}

public class BackgroundEventQueue : IBackgroundEventQueue
{
    private readonly ConcurrentQueue<AnalyticsEvent> _queue = new();
    private readonly SemaphoreSlim _signal = new(0);

    public void Enqueue(List<AnalyticsEvent> evnts)
    {
        foreach (var evt in evnts)
        {
            _queue.Enqueue(evt);
        }
        _signal.Release();
    }

    public void Enqueue(AnalyticsEvent evt)
    {
        _queue.Enqueue(evt);
        _signal.Release();
    }

    public IReadOnlyList<AnalyticsEvent> DequeueAll()
    {
        const int batchMaxSize = 1_000;
        var list = new List<AnalyticsEvent>(batchMaxSize);
        while (_queue.TryDequeue(out var evt) && list.Count < batchMaxSize)
        {
            list.Add(evt);
        }
        return list;
    }
}