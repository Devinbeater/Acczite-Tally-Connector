using System;

namespace Acczite20.Models
{
    public class BillAllocation : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public Guid LedgerId { get; set; }
        public string BillName { get; set; } = string.Empty;
        public string BillType { get; set; } = string.Empty; // New Ref, Advance, Agst Ref, On Account
        public decimal Amount { get; set; }
        
        public virtual Voucher Voucher { get; set; }
    }
}
