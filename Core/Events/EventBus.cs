using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Acczite20.Core.Events
{
    public interface IIntegrationEvent 
    { 
        string CorrelationId { get; set; }
    }

    public abstract class BaseIntegrationEvent : IIntegrationEvent
    {
        public string CorrelationId { get; set; } = Guid.NewGuid().ToString();
    }

    public class VoucherCreatedEvent : BaseIntegrationEvent
    {
        public Guid VoucherId { get; set; }
        public string VoucherNumber { get; set; }
        public string PartyName { get; set; }
        public decimal Amount { get; set; }
        public VoucherCreatedEvent(Guid voucherId, string voucherNumber, string partyName, decimal amount)
        {
            VoucherId = voucherId;
            VoucherNumber = voucherNumber;
            PartyName = partyName;
            Amount = amount;
        }
    }

    public class StockChangedEvent : BaseIntegrationEvent
    {
        public Guid StockItemId { get; set; }
        public string ItemName { get; set; }
        public double QuantityChange { get; set; }
        public StockChangedEvent(Guid stockItemId, string itemName, double quantityChange)
        {
            StockItemId = stockItemId;
            ItemName = itemName;
            QuantityChange = quantityChange;
        }
    }

    public class EmployeeCreatedEvent : BaseIntegrationEvent
    {
        public string EmployeeId { get; set; }
        public string OrganizationId { get; set; }
        public string EmployeeName { get; set; }
        public EmployeeCreatedEvent(string employeeId, string organizationId, string employeeName)
        {
            EmployeeId = employeeId;
            OrganizationId = organizationId;
            EmployeeName = employeeName;
        }
    }

    public class AttendanceMarkedEvent : BaseIntegrationEvent
    {
        public string EmployeeId { get; set; }
        public string OrganizationId { get; set; }
        public DateTime Date { get; set; }
        public string Status { get; set; }
        public AttendanceMarkedEvent(string employeeId, string organizationId, DateTime date, string status)
        {
            EmployeeId = employeeId;
            OrganizationId = organizationId;
            Date = date;
            Status = status;
        }
    }

    public class MappingApprovedEvent : BaseIntegrationEvent
    {
        public string EntityType { get; set; }
        public string MernName { get; set; }
        public string TallyName { get; set; }
        public MappingApprovedEvent(string entityType, string mernName, string tallyName)
        {
            EntityType = entityType;
            MernName = mernName;
            TallyName = tallyName;
        }
    }

    public class BatchSyncCompletedEvent : BaseIntegrationEvent
    {
        public string EntityType { get; set; }
        public int Count { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public BatchSyncCompletedEvent(string entityType, int count)
        {
            EntityType = entityType;
            Count = count;
            Timestamp = DateTimeOffset.UtcNow;
        }
    }

    public static class EventBus
    {
        private static readonly ConcurrentDictionary<Type, List<Func<IIntegrationEvent, Task>>> _handlers = new();

        /// <summary>
        /// Optional callback invoked when an event handler throws. Wire this up in App.xaml.cs.
        /// Signature: (eventTypeName, correlationId, exception)
        /// </summary>
        public static Action<string, string, Exception>? OnHandlerError { get; set; }

        public static void Subscribe<TEvent>(Func<TEvent, Task> handler) where TEvent : IIntegrationEvent
        {
            var eventType = typeof(TEvent);
            if (!_handlers.ContainsKey(eventType))
                _handlers[eventType] = new List<Func<IIntegrationEvent, Task>>();

            _handlers[eventType].Add(e => handler((TEvent)e));
        }

        public static async Task PublishAsync<TEvent>(TEvent @event) where TEvent : IIntegrationEvent
        {
            var eventType = typeof(TEvent);
            if (!_handlers.TryGetValue(eventType, out var handlers))
                return;

            foreach (var handler in handlers)
            {
                try
                {
                    await handler(@event);
                }
                catch (Exception ex)
                {
                    OnHandlerError?.Invoke(eventType.Name, @event.CorrelationId, ex);
                }
            }
        }
    }
}
