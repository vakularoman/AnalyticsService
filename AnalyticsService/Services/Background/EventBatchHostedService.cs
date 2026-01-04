namespace AnalyticsService.Services.Background;

public class EventBatchHostedService : BackgroundService
{
    private readonly IBackgroundEventQueue _queue;
    private readonly AnalyticsEventService _service;
    private readonly TimeSpan _flushInterval = TimeSpan.FromSeconds(5);

    public EventBatchHostedService(IBackgroundEventQueue queue, AnalyticsEventService service)
    {
        _queue = queue;
        _service = service;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_flushInterval, stoppingToken);

                var batch = _queue.DequeueAll();
                if (batch.Count > 0)
                {
                    await _service.InsertEventsBatchAsync(batch);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        var remaining = _queue.DequeueAll();
        if (remaining.Count > 0)
        {
            await _service.InsertEventsBatchAsync(remaining);
        }
    }
}