namespace AnalyticsService.DTO;

public class AnalyticsEventDto
{
    public string EventType { get; set; } = null!;

    public ulong UserId { get; set; }

    public Guid SessionId { get; set; }

    public string? ElementId { get; set; }

    public string? ElementClass { get; set; }

    public uint? DurationSeconds { get; set; }

    public string? FormName { get; set; }
}
