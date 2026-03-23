using System;

namespace Acczite20.Models
{
    /// <summary>
    /// Tracks incremental sync state per entity type per company.
    /// Supports crash recovery, retry logic, and health monitoring.
    /// </summary>
    public class SyncMetadata : BaseEntity
    {
        public Guid CompanyId { get; set; }
        public string EntityType { get; set; } = string.Empty;
        
        // ── Incremental Tracking ──
        public DateTimeOffset LastSyncDate { get; set; }
        public string? LastTallyMasterId { get; set; }
        public DateTimeOffset LastModified { get; set; }
        public string? LastVoucherNumber { get; set; }
        public string? LastVoucherMasterId { get; set; }

        // ── Crash Recovery & Health ──
        public DateTimeOffset? LastSuccessfulSync { get; set; }
        public string? LastError { get; set; }
        public int RetryCount { get; set; }
        public bool IsSyncRunning { get; set; }
        public int RecordsSyncedInLastRun { get; set; }

        public virtual Company Company { get; set; } = null!;
    }
}
