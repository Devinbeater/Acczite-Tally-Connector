using System;

namespace Acczite20.Models.Analytics
{
    public class BusinessPulseSnapshot : BaseEntity
    {
        public DateTime Date { get; set; }
        public decimal SalesToday { get; set; }
        public int InvoicesToday { get; set; }
        public decimal CollectionsToday { get; set; }
        public decimal Receivables { get; set; }
        public decimal Payables { get; set; }
        public decimal CashPosition { get; set; }
        public int AlertCount { get; set; }
        public DateTime LastSyncedAt { get; set; }
    }
}
