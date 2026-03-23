using System;

namespace Acczite20.Models
{
    public class ContraEntry : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string FromAccount { get; set; }
        public string ToAccount { get; set; }
        public decimal Amount { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
