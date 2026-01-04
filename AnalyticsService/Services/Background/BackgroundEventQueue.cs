using AnalyticsService.DTO;
using System.Collections.Concurrent;

namespace AnalyticsService.Services.Background;

public interface IBackgroundEventQueue
{
    void Enqueue(AnalyticsEventDto evt);

    IReadOnlyList<AnalyticsEventDto> DequeueAll();
}

public class BackgroundEventQueue : IBackgroundEventQueue
{
    private readonly ConcurrentQueue<AnalyticsEventDto> _queue = new();
    private readonly SemaphoreSlim _signal = new(0);

    public void Enqueue(AnalyticsEventDto evt)
    {
        _queue.Enqueue(evt);
        _signal.Release();
    }

    public IReadOnlyList<AnalyticsEventDto> DequeueAll()
    {
        var list = new List<AnalyticsEventDto>();
        while (_queue.TryDequeue(out var evt))
        {
            list.Add(evt);
        }
        return list;
    }
}