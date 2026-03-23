using System;

namespace Acczite20.Models
{
    public class GstBreakdown : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string TaxType { get; set; } = string.Empty; // e.g. CGST, SGST, IGST
        public decimal AssessableValue { get; set; }
        public decimal TaxRate { get; set; }
        public decimal TaxAmount { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
