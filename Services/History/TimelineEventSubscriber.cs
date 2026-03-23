using System;
using System.Text.Json;
using System.Threading.Tasks;
using Acczite20.Core.Events;
using Acczite20.Data;
using Acczite20.Models.History;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Services.History
{
    public static class TimelineEventSubscriber
    {
        public static void RegisterAll(IServiceProvider services)
        {
            EventBus.Subscribe<VoucherCreatedEvent>(async e => await LogActivityAsync(services, e, "Tally", "Voucher", "Voucher Created", $"Voucher #{e.VoucherNumber} created for {e.PartyName} (Total: {e.Amount})"));
            EventBus.Subscribe<StockChangedEvent>(async e => await LogActivityAsync(services, e, "Tally", "StockItem", "Stock Changed", $"Stock for {e.ItemName} adjusted by {e.QuantityChange}"));
            EventBus.Subscribe<EmployeeCreatedEvent>(async e => await LogActivityAsync(services, e, "MERN", "Employee", "Employee Created", $"Portal user created: {e.EmployeeName}"));
            EventBus.Subscribe<AttendanceMarkedEvent>(async e => await LogActivityAsync(services, e, "MERN", "Attendance", "Attendance Marked", $"Attendance for Employee ID {e.EmployeeId} logged as {e.Status}"));
            EventBus.Subscribe<MappingApprovedEvent>(async e => await LogActivityAsync(services, e, "WPF", e.EntityType, "Mapping Approved", $"Cross-platform link confirmed: {e.MernName} (Cloud) -> {e.TallyName} (Tally)"));
            EventBus.Subscribe<BatchSyncCompletedEvent>(async e => await LogActivityAsync(services, e, "Tally", e.EntityType, "Sync Completed", $"Successfully ingested batch of {e.Count} {e.EntityType} into Warehouse."));
        }

        private static async Task LogActivityAsync(IServiceProvider services, object evt, string source, string entityType, string eventType, string description, string severity = "Info")
        {
            try
            {
                using var scope = services.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                string correlationId = (evt as IIntegrationEvent)?.CorrelationId;

                var activity = new UnifiedActivityLog
                {
                    Id = Guid.NewGuid(),
                    OrganizationId = SessionManager.Instance.OrganizationId,
                    CreatedAt = DateTimeOffset.UtcNow,
                    UpdatedAt = DateTimeOffset.UtcNow,
                    SourceSystem = source,
                    EntityType = entityType,
                    EventType = eventType,
                    Description = description,
                    Severity = severity,
                    CorrelationId = correlationId,
                    Timestamp = DateTimeOffset.UtcNow,
                    MetadataJson = JsonSerializer.Serialize(evt, evt.GetType())
                };

                db.UnifiedActivityLogs.Add(activity);
                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                // We don't want timeline logging to crash the system
                System.Diagnostics.Debug.WriteLine($"Timeline logging failed: {ex.Message}");
            }
        }
    }
}
