using System;

namespace Acczite20.Models.Warehouse
{
    public class LedgerMapping : BaseEntity
    {
        public Guid LedgerId { get; set; }
        public string MappedCategory { get; set; } // Sales, Receivables, Payables, Cash, Expense, Income
        public string TallyLedgerName { get; set; }
    }
}
