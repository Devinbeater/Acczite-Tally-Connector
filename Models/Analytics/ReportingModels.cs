using System;
using System.Collections.Generic;

namespace Acczite20.Models.Analytics
{
    public class VarianceReportRow
    {
        public Guid LedgerId { get; set; }
        public string LedgerName { get; set; }
        public string GroupName { get; set; }
        public decimal Period1Amount { get; set; }
        public decimal Period2Amount { get; set; }
        public decimal VarianceAmount => Period2Amount - Period1Amount;
        public decimal VariancePercentage => Period1Amount != 0 ? (VarianceAmount / Math.Abs(Period1Amount)) * 100 : 0;
    }

    public class AnomalyRecord
    {
        public string Title { get; set; }
        public string Description { get; set; }
        public string Severity { get; set; } // Low, Medium, High
        public DateTimeOffset DetectedAt { get; set; }
        public Guid? RelatedVoucherId { get; set; }
        public string RelatedEntityName { get; set; }
    }
}
