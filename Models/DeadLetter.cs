using System;

namespace Acczite20.Models
{
    public class DeadLetter : BaseEntity
    {
        public Guid CompanyId { get; set; }
        public string EntityType { get; set; } = string.Empty; // e.g., "Voucher"
        public string TallyMasterId { get; set; } = string.Empty;
        public string ErrorReason { get; set; } = string.Empty;
        public string? PayloadXml { get; set; }
        public bool IsResolved { get; set; } = false;
        public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;

        public virtual Company Company { get; set; } = null!;
    }
}
