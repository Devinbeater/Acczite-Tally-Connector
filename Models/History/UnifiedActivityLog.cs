using System;
using System.ComponentModel.DataAnnotations;

namespace Acczite20.Models.History
{
    public class UnifiedActivityLog : BaseEntity
    {
        public string? SourceSystem { get; set; } // Tally, MERN, WPF
        
        public string? EntityType { get; set; } // Voucher, StockItem, Employee, etc.
        
        public string? EntityId { get; set; }
        
        public string? EventType { get; set; } // Created, Updated, Deleted, Sync, etc.
        
        public string? Description { get; set; }
        
        public DateTimeOffset Timestamp { get; set; }
        
        public string? UserId { get; set; }
        
        public string Severity { get; set; } = "Info"; // Info, Warning, Critical
        
        public string? CorrelationId { get; set; } // To group related business events
        
        public string? MetadataJson { get; set; } // Optional JSON for extra details
    }
}
