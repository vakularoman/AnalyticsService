using AnalyticsService.Models;
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

    [HttpPost]
    public IActionResult CreateEvent([FromBody] AnalyticsEvent analyticsEvent)
    {
        if (string.IsNullOrWhiteSpace(analyticsEvent.EventType))
        {
            return BadRequest("EventType is required");
        }

        _queue.Enqueue(analyticsEvent);
        return Accepted();
    }

    [HttpGet("statistics")]
    public async Task<IActionResult> GetVideoStatistics(
        [FromQuery] string vslName,
        [FromQuery] DateTime startDate,
        [FromQuery] DateTime endDate)
    {
        if (string.IsNullOrWhiteSpace(vslName))
        {
            return BadRequest("vslName is required");
        }

        if (startDate > endDate)
        {
            return BadRequest("startDate must be earlier than endDate");
        }

        var stats = await _service.GetVideoStatistics(vslName, startDate, endDate);
        return Ok(stats);
    }

    [HttpGet("get-vsl")]
    public async Task<IActionResult> GetVlsByBuyerName([FromQuery] string[] buyerNames)
    {
        if (buyerNames.Length == 0)
        {
            return BadRequest("At least one buyerName is required");
        }

        var vslNames = await _service.GetVlsByBuyerName(buyerNames);
        return Ok(vslNames);
    }
}
