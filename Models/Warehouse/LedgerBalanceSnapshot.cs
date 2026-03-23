using System;

namespace Acczite20.Models.Warehouse
{
    public class LedgerBalanceSnapshot : BaseEntity
    {
        public Guid LedgerId { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public decimal DebitTotal { get; set; }
        public decimal CreditTotal { get; set; }
        public decimal ClosingBalance { get; set; }
    }
}
