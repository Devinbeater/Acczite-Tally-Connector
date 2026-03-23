using System;
using System.Collections.Generic;

namespace Acczite20.Models
{
    public class Voucher : BaseEntity
    {
        public Guid CompanyId { get; set; }
        public string VoucherNumber { get; set; }
        public Guid VoucherTypeId { get; set; }
        public DateTimeOffset VoucherDate { get; set; }
        public string ReferenceNumber { get; set; }
        public string Narration { get; set; }
        public decimal TotalAmount { get; set; }
        public bool IsCancelled { get; set; }
        public bool IsOptional { get; set; }
        public string TallyMasterId { get; set; }
        public int AlterId { get; set; } // Tally versioning number
        public DateTimeOffset LastModified { get; set; }
        public Guid? SyncRunId { get; set; } // Temporary ID to track record status during a sync run

        public virtual Company Company { get; set; }
        public virtual VoucherType VoucherType { get; set; }
        public virtual ICollection<LedgerEntry> LedgerEntries { get; set; }
        public virtual ICollection<InventoryAllocation> InventoryAllocations { get; set; }
        public virtual ICollection<GstBreakdown> GstBreakdowns { get; set; }
    }
}
