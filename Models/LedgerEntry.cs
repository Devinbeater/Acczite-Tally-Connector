using System;

namespace Acczite20.Models
{
    public class LedgerEntry : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string LedgerName { get; set; }
        public string LedgerGroup { get; set; }
        public decimal DebitAmount { get; set; }
        public decimal CreditAmount { get; set; }
        public bool IsPartyLedger { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
