using System;

namespace Acczite20.Models
{
    public abstract class BaseEntity
    {
        public Guid Id { get; set; }
        public Guid OrganizationId { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset UpdatedAt { get; set; }
        public bool IsDeleted { get; set; }
        public bool IsActive { get; set; } = true;
        public bool IsPresentInTally { get; set; } = true; // Tracks if the record still exists in the source Tally company
        public long TallyAlterId { get; set; } // Source Alter ID for change detection and drift-proofing
    }
}
