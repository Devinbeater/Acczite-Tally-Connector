using System;

namespace Acczite20.Models.Integration
{
    public class IntegrationAuditLog : BaseEntity
    {
        public string? EventType { get; set; } // e.g., VoucherCreated, StockChanged, EmployeeSynced
        public string? SourceSystem { get; set; } // Tally, MERN, Warehouse
        public string? TargetSystem { get; set; } // MERN, Tally, Warehouse
        public string? EntityId { get; set; } // The ID of the affected record
        public string? Status { get; set; } // Success, Failed, Pending
        public string? ErrorMessage { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class PendingMapping : BaseEntity
    {
        public string? EntityType { get; set; } // Product, Employee, Godown
        public string? MernId { get; set; } 
        public string? MernDisplayName { get; set; }
        public string? SuggestedTallyGuid { get; set; }
        public string? SuggestedTallyName { get; set; }
        public decimal ConfidenceScore { get; set; } // 0.0 to 1.0
        public string? Status { get; set; } // Pending, Confirmed, Rejected
        public DateTime CreatedAt { get; set; }
    }

    public class IntegrationEventQueue : BaseEntity
    {
        public string? EventType { get; set; }
        public string? Payload { get; set; }
        public int RetryCount { get; set; }
        public string? Status { get; set; } // Pending, Processing, Completed, Failed
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset? LastAttempt { get; set; }
    }
}
