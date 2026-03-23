using System;

namespace Acczite20.Models
{
    public class Invoice : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string InvoiceNumber { get; set; }
        public string CustomerName { get; set; }
        public string GSTNumber { get; set; }
        public decimal TaxableAmount { get; set; }
        public decimal CGST { get; set; }
        public decimal SGST { get; set; }
        public decimal IGST { get; set; }
        public decimal GrandTotal { get; set; }
        public DateTimeOffset DueDate { get; set; }
        public string Status { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
