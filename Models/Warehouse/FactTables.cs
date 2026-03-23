using System;

namespace Acczite20.Models.Warehouse
{
    public class FactVoucher : BaseEntity
    {
        public Guid CompanyId { get; set; }
        public string VoucherNumber { get; set; }
        public Guid VoucherTypeId { get; set; }
        public DateTimeOffset VoucherDate { get; set; }
        public decimal TotalAmount { get; set; }
        public string TallyMasterId { get; set; }
        public int AlterId { get; set; }
        public bool IsCancelled { get; set; }
        public bool IsOptional { get; set; }
    }

    public class FactNarration : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string Narration { get; set; }
    }

    public class FactLedgerEntry : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public Guid LedgerId { get; set; }
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
        public DateTimeOffset VoucherDate { get; set; } // Denormalized for partitioning
    }

    public class FactInventoryMovement : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public Guid StockItemId { get; set; }
        public decimal Quantity { get; set; }
        public decimal Rate { get; set; }
        public decimal Amount { get; set; }
        public Guid? GodownId { get; set; }
        public DateTimeOffset VoucherDate { get; set; } // Denormalized
    }

    public class FactTaxEntry : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public Guid LedgerId { get; set; }
        public string TaxType { get; set; }
        public decimal TaxRate { get; set; }
        public decimal TaxAmount { get; set; }
    }
}
