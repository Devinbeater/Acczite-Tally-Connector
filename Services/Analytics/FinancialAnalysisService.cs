using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Analytics;
using Acczite20.Models.Warehouse;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Analytics
{
    public interface IFinancialAnalysisService
    {
        Task<List<VarianceReportRow>> GetComparativeProfitLossAsync(Guid orgId, int year1, int year2);
        Task<List<AnomalyRecord>> GetAnomaliesAsync(Guid orgId);
        Task<List<BudgetVsActualRow>> GetBudgetVsActualAsync(Guid orgId, int year, int month);
        Task<List<ReceivableRiskRow>> GetReceivableRisksAsync(Guid orgId);
    }

    public class FinancialAnalysisService : IFinancialAnalysisService
    {
        private readonly AppDbContext _context;

        public FinancialAnalysisService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<List<VarianceReportRow>> GetComparativeProfitLossAsync(Guid orgId, int year1, int year2)
        {
            var snapshots = await _context.LedgerBalanceSnapshots
                .Where(s => s.OrganizationId == orgId && (s.Year == year1 || s.Year == year2))
                .ToListAsync();

            var ledgers = await _context.DimLedgers
                .Where(l => l.OrganizationId == orgId)
                .ToDictionaryAsync(l => l.Id, l => l.LedgerName);

            var report = snapshots
                .GroupBy(s => s.LedgerId)
                .Select(g => new VarianceReportRow
                {
                    LedgerId = g.Key,
                    LedgerName = ledgers.ContainsKey(g.Key) ? ledgers[g.Key] : "Unknown",
                    Period1Amount = g.Where(s => s.Year == year1).Sum(s => s.CreditTotal - s.DebitTotal),
                    Period2Amount = g.Where(s => s.Year == year2).Sum(s => s.CreditTotal - s.DebitTotal)
                })
                .OrderByDescending(r => Math.Abs(r.VarianceAmount))
                .ToList();

            return report;
        }

        public async Task<List<AnomalyRecord>> GetAnomaliesAsync(Guid orgId)
        {
            var anomalies = new List<AnomalyRecord>();

            try
            {
                var currentMonth = DateTime.Now.Month;
                var currentYear = DateTime.Now.Year;
                
                var monthSnapshots = await _context.LedgerBalanceSnapshots
                    .Where(s => s.OrganizationId == orgId && s.Year == currentYear)
                    .ToListAsync();

                var ledgers = await _context.DimLedgers
                    .Where(l => l.OrganizationId == orgId)
                    .ToDictionaryAsync(l => l.Id, l => l.LedgerName);

                foreach(var g in monthSnapshots.GroupBy(s => s.LedgerId))
                {
                    var current = g.FirstOrDefault(s => s.Month == currentMonth)?.DebitTotal ?? 0;
                    var avg = g.Where(s => s.Month != currentMonth).Select(s => (decimal?)s.DebitTotal).Average() ?? 0;
                    
                    if (current > avg * 2 && avg > 1000) 
                    {
                        var ledgerName = ledgers.ContainsKey(g.Key) ? ledgers[g.Key] : g.Key.ToString();
                        anomalies.Add(new AnomalyRecord
                        {
                            Title = "Expense Spike Detected",
                            Severity = "High",
                            DetectedAt = DateTimeOffset.UtcNow,
                            RelatedEntityName = ledgerName,
                            Description = $"{ledgerName} spend of ₹{current:N2} is significantly higher than monthly average of ₹{avg:N2}."
                        });
                    }
                }
            }
            catch { }

            return anomalies;
        }

        public async Task<List<ReceivableRiskRow>> GetReceivableRisksAsync(Guid orgId)
        {
            // Find Receivable mappings
            var mappings = await _context.LedgerMappings
                .Where(m => m.OrganizationId == orgId && m.MappedCategory == "Receivables")
                .Select(m => m.LedgerId)
                .ToListAsync();

            if (!mappings.Any()) return new List<ReceivableRiskRow>();

            // Calculate ageing from vouchers
            var risks = await _context.FactLedgerEntries
                .Where(e => e.OrganizationId == orgId && mappings.Contains(e.LedgerId))
                .GroupBy(e => e.LedgerId)
                .Select(g => new {
                    LedgerId = g.Key,
                    Balance = g.Sum(e => e.Debit - e.Credit),
                    LastVoucherDate = g.Max(e => e.VoucherDate)
                })
                .Where(x => x.Balance > 1000)
                .OrderByDescending(x => x.Balance)
                .ToListAsync();

            var ledgers = await _context.DimLedgers
                .Where(l => l.OrganizationId == orgId && mappings.Contains(l.Id))
                .ToDictionaryAsync(l => l.Id, l => l.LedgerName);

            return risks.Select(r => new ReceivableRiskRow
            {
                LedgerName = ledgers.ContainsKey(r.LedgerId) ? ledgers[r.LedgerId] : "Unknown",
                Balance = r.Balance,
                DaysOutstanding = (int)(DateTimeOffset.UtcNow - r.LastVoucherDate).TotalDays,
                RiskLevel = (DateTimeOffset.UtcNow - r.LastVoucherDate).TotalDays > 120 ? "Critical" : 
                            (DateTimeOffset.UtcNow - r.LastVoucherDate).TotalDays > 90 ? "High" : "Medium"
            }).ToList();
        }

        public async Task<List<BudgetVsActualRow>> GetBudgetVsActualAsync(Guid orgId, int year, int month)
        {
            var snapshots = await _context.LedgerBalanceSnapshots
                .Where(s => s.OrganizationId == orgId && s.Year == year - 1)
                .ToListAsync();

            var currentMonthSnapshots = await _context.LedgerBalanceSnapshots
                .Where(s => s.OrganizationId == orgId && s.Year == year && s.Month == month)
                .ToDictionaryAsync(s => s.LedgerId, s => s);

            var ledgers = await _context.DimLedgers
                .Where(l => l.OrganizationId == orgId)
                .ToDictionaryAsync(l => l.Id, l => l.LedgerName);

            var rows = snapshots.GroupBy(s => s.LedgerId)
                .Select(g => {
                    var actualVal = currentMonthSnapshots.ContainsKey(g.Key) ? currentMonthSnapshots[g.Key].DebitTotal : 0;
                    var budgetVal = (g.Average(s => s.DebitTotal)) * 1.05m;
                    
                    return new BudgetVsActualRow
                    {
                        LedgerName = ledgers.ContainsKey(g.Key) ? ledgers[g.Key] : "Unknown",
                        BudgetAmount = budgetVal,
                        ActualAmount = actualVal
                    };
                })
                .Where(r => r.BudgetAmount > 0 || r.ActualAmount > 0)
                .OrderByDescending(r => r.VarianceAmount)
                .ToList();

            return rows;
        }
    }

    public class BudgetVsActualRow
    {
        public string LedgerName { get; set; }
        public decimal BudgetAmount { get; set; }
        public decimal ActualAmount { get; set; }
        public decimal VarianceAmount => ActualAmount - BudgetAmount;
        public decimal VariancePercent => BudgetAmount != 0 ? (VarianceAmount / BudgetAmount) * 100 : 0;
    }
}
