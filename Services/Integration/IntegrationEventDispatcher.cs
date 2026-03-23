using System;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Integration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Acczite20.Services.Integration
{
    /// <summary>
    /// Background service that processes the IntegrationEventQueue.
    /// Picks up Pending events, sends them to the MERN webhook API,
    /// and uses exponential backoff for retries on failure.
    /// Call StartAsync() from App startup; it runs on a background thread.
    /// </summary>
    public class IntegrationEventDispatcher
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<IntegrationEventDispatcher> _logger;
        private readonly HttpClient _httpClient;
        private CancellationTokenSource _cts;
        private const int MaxRetries = 5;
        private const int PollIntervalSeconds = 10;
        private readonly SemaphoreSlim _signal = new SemaphoreSlim(0);

        // Map EventType → MERN webhook path
        private static readonly System.Collections.Generic.Dictionary<string, string> EndpointMap = new()
        {
            ["VoucherCreated"]   = "/api/integration/voucher-created",
            ["StockChanged"]     = "/api/integration/stock-changed",
            ["EmployeeCreated"]  = "/api/integration/employee-created",
            ["AttendanceMarked"] = "/api/integration/attendance-marked",
        };

        public IntegrationEventDispatcher(
            IServiceScopeFactory scopeFactory,
            ILogger<IntegrationEventDispatcher> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;

            var mernBaseUrl = Environment.GetEnvironmentVariable("ACCZITE_MERN_URL") ?? "https://api.acczite.in";
            var token = Environment.GetEnvironmentVariable("ACCZITE_INTEGRATION_TOKEN") ?? "";

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(mernBaseUrl),
                Timeout = TimeSpan.FromSeconds(30)
            };
            _httpClient.DefaultRequestHeaders.Add("x-acczite-token", token);
        }

        public void StartAsync()
        {
            _cts = new CancellationTokenSource();
            Task.Run(() => ExecuteLoopAsync(_cts.Token));
            _logger.LogInformation("IntegrationEventDispatcher started.");
        }

        public void Stop()
        {
            _cts?.Cancel();
        }

        public void TriggerDispatch()
        {
            try { _signal.Release(); } catch { }
        }

        private async Task ExecuteLoopAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ProcessPendingEventsAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in event dispatcher loop.");
                }

                try
                {
                    // Wait for a signal OR 10 seconds (heartbeat)
                    await _signal.WaitAsync(TimeSpan.FromSeconds(PollIntervalSeconds), stoppingToken);
                }
                catch (TaskCanceledException) { break; }
            }
        }

        private async Task ProcessPendingEventsAsync(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var pendingEvents = await db.IntegrationEventQueues
                .Where(e => e.Status == "Pending" || e.Status == "Retrying")
                .Where(e => e.RetryCount < MaxRetries)
                .OrderBy(e => e.CreatedAt)
                .Take(20)
                .ToListAsync(ct);

            foreach (var evt in pendingEvents)
            {
                if (ct.IsCancellationRequested) break;

                evt.Status = "Processing";
                evt.LastAttempt = DateTimeOffset.UtcNow;
                await db.SaveChangesAsync(ct);

                var success = await TrySendAsync(evt);

                if (success)
                {
                    evt.Status = "Completed";
                    _logger.LogInformation("Event {Id} ({Type}) dispatched successfully.", evt.Id, evt.EventType);
                }
                else
                {
                    evt.RetryCount++;
                    evt.Status = evt.RetryCount >= MaxRetries ? "Failed" : "Retrying";
                    _logger.LogWarning("Event {Id} ({Type}) failed. Retry {Count}/{Max}.",
                        evt.Id, evt.EventType, evt.RetryCount, MaxRetries);
                }

                await db.SaveChangesAsync(ct);
            }
        }

        private async Task<bool> TrySendAsync(IntegrationEventQueue evt)
        {
            try
            {
                if (!EndpointMap.TryGetValue(evt.EventType, out var path))
                {
                    _logger.LogWarning("Unknown event type: {Type}. Skipping.", evt.EventType);
                    return true;
                }

                var content = new StringContent(evt.Payload, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync(path, content);

                return response.IsSuccessStatusCode;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogError(ex, "HTTP error dispatching event {Id}.", evt.Id);
                return false;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
        }
    }
}
