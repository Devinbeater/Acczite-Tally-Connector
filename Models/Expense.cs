using System;

namespace Acczite20.Models
{
    public class Expense : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string ExpenseCategory { get; set; }
        public decimal Amount { get; set; }
        public string PaymentMode { get; set; }
        public string Remarks { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
