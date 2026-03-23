using System;
using System.Collections.Generic;

namespace Acczite20.Models.Analytics
{
    public class BusinessPulseStats
    {
        public decimal SalesToday { get; set; }
        public int InvoicesToday { get; set; }
        public decimal CollectionsToday { get; set; }
        
        public decimal TotalReceivables { get; set; }
        public decimal TotalPayables { get; set; }
        public decimal NetCashBank { get; set; }

        public decimal SalesGrowth { get; set; } // Variance vs yesterday or last week
        public decimal ExpenseSpike { get; set; }
        public string TopExpenseLedger { get; set; }

        public List<PulseAlert> Alerts { get; set; } = new();
    }

    public class PulseAlert
    {
        public string Message { get; set; }
        public string Severity { get; set; } // Info, Warning, Critical
        public string Category { get; set; } // Cash, Sales, Inventory
    }
}
