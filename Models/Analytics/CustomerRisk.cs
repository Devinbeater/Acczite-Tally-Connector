using System;
using System.Collections.Generic;

namespace Acczite20.Models.Analytics
{
    public class CustomerRiskProfile
    {
        public Guid LedgerId { get; set; }
        public string CustomerName { get; set; }
        public decimal TotalOutstanding { get; set; }
        public decimal OverdueAmount { get; set; }
        public int AveragePaymentDays { get; set; }
        public string RiskLevel { get; set; } // Low, Medium, High, Critical
        public string RiskReason { get; set; }
        public List<RiskIndicator> Indicators { get; set; } = new();
    }

    public class RiskIndicator
    {
        public string Type { get; set; } // Aging, Volume, PaymentDelay
        public string Status { get; set; } // Good, Warning, Bad
        public string Message { get; set; }
    }
}
