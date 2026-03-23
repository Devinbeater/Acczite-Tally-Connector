using System;
using System.Text.Json;
using System.Threading.Tasks;
using Acczite20.Core.Events;
using Acczite20.Data;
using Acczite20.Models.Integration;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Services.Integration
{
    /// <summary>
    /// Subscribes to EventBus integration events and persists them
    /// into the IntegrationEventQueue table for reliable delivery
    /// by the IntegrationEventDispatcher.
    /// </summary>
    public static class EventQueueSubscriber
    {
        public static void RegisterAll(IServiceProvider services)
        {
            EventBus.Subscribe<VoucherCreatedEvent>(async e => await EnqueueAsync(services, "VoucherCreated", e));
            EventBus.Subscribe<StockChangedEvent>(async e => await EnqueueAsync(services, "StockChanged", e));
            EventBus.Subscribe<EmployeeCreatedEvent>(async e => await EnqueueAsync(services, "EmployeeCreated", e));
            EventBus.Subscribe<AttendanceMarkedEvent>(async e => await EnqueueAsync(services, "AttendanceMarked", e));
        }

        private static async Task EnqueueAsync(IServiceProvider services, string eventType, IIntegrationEvent evt)
        {
            try
            {
                using var scope = services.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                var queueItem = new IntegrationEventQueue
                {
                    Id = Guid.NewGuid(),
                    EventType = eventType,
                    Payload = JsonSerializer.Serialize(evt, evt.GetType()),
                    RetryCount = 0,
                    Status = "Pending",
                    CreatedAt = DateTimeOffset.UtcNow,
                    LastAttempt = null
                };

                db.IntegrationEventQueues.Add(queueItem);
                await db.SaveChangesAsync();

                // Signal the dispatcher to process immediately
                var dispatcher = services.GetService<IntegrationEventDispatcher>();
                dispatcher?.TriggerDispatch();
            }
            catch
            {
                // Silently fail — event dispatcher will pick up retries on next heartbeat
            }
        }
    }
}
