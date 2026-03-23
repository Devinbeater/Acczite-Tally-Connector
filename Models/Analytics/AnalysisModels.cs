using System;

namespace Acczite20.Models.Analytics
{
    public class ReceivableRiskRow
    {
        public string LedgerName { get; set; }
        public decimal Balance { get; set; }
        public int DaysOutstanding { get; set; }
        public string RiskLevel { get; set; } // High, Medium, Low
    }
}
