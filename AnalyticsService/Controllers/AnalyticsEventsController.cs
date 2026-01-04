using AnalyticsService.DTO;
using AnalyticsService.Services;
using AnalyticsService.Services.Background;
using Microsoft.AspNetCore.Mvc;

namespace AnalyticsService.Controllers;

[ApiController]
[Route("api/events")]
public class AnalyticsEventsController : ControllerBase
{
    private readonly IBackgroundEventQueue _queue;
    private readonly AnalyticsEventService _service;

    public AnalyticsEventsController(IBackgroundEventQueue queue, AnalyticsEventService service)
    {
        _queue = queue;
        _service = service;
    }

    [HttpGet]
    public async Task<IActionResult> GetLastEvents([FromQuery] int count = 10)
    {
        var events = await _service.GetLastEventsAsync(count);
        return Ok(events);
    }

    [HttpPost]
    public IActionResult CreateEvent([FromBody] AnalyticsEventDto dto)
    {
        if (string.IsNullOrWhiteSpace(dto.EventType))
        {
            return BadRequest("EventType is required");
        }

        _queue.Enqueue(dto);
        return Accepted();
    }
}
