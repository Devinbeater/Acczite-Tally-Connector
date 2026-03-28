using System;

namespace Acczite20.Models
{
    public enum DeadLetterFailureType
    {
        MissingMaster,    // Retryable (Master Sync resolve)
        ValidationError,  // Non-retryable (Format issue)
        Transient,       // Retryable (Network/DB issue)
        Duplicate        // Non-retryable (Ignored)
    }

    public class DeadLetter : BaseEntity
    {
        public Guid CompanyId { get; set; }
        public string EntityType { get; set; } = string.Empty; // e.g., "Voucher"
        public string TallyMasterId { get; set; } = string.Empty;
        public string ErrorReason { get; set; } = string.Empty;
        public string? PayloadXml { get; set; }
        public bool IsResolved { get; set; } = false;
        public int RetryCount { get; set; } = 0;
        public DateTimeOffset? LastAttemptAt { get; set; }
        public DeadLetterFailureType FailureType { get; set; } = DeadLetterFailureType.MissingMaster;
        public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;

        public virtual Company Company { get; set; } = null!;
    }
}
