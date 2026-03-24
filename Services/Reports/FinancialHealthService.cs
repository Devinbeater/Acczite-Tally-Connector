using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Reports
{
    public enum HealthStatus { Green, Yellow, Red }

    public class DiagnosticItem
    {
        public string Description { get; set; } = string.Empty;
        public HealthStatus Severity { get; set; }
    }

    public class UnbalancedVoucherDiag
    {
        public string VoucherNumber { get; set; } = string.Empty;
        public DateTimeOffset VoucherDate { get; set; }
        public string VoucherType { get; set; } = string.Empty;
        public decimal Imbalance { get; set; }
    }

    public class OrphanLedgerDiag
    {
        public string LedgerName { get; set; } = string.Empty;
        public string ParentGroup { get; set; } = string.Empty;
    }

    public class FinancialHealthResult
    {
        // ── Trial Balance ──
        public HealthStatus TBStatus { get; set; }
        public decimal TBTotalDebit { get; set; }
        public decimal TBTotalCredit { get; set; }
        public decimal TBDifference { get; set; }

        // ── Balance Sheet ──
        public HealthStatus BSStatus { get; set; }
        public decimal BSAssets { get; set; }
        public decimal BSLiabilities { get; set; }
        public decimal BSDifference { get; set; }
        public string BSDifferenceLabel { get; set; } = string.Empty;

        // ── P&L ──
        public decimal NetProfit { get; set; }

        // ── Overall ──
        public HealthStatus OverallStatus { get; set; }
        public List<string> Issues { get; set; } = new();
        public DateTimeOffset GeneratedAt { get; set; }

        // ── Drill-down diagnostics (populated only when Red/Yellow) ──
        public List<DiagnosticItem> Diagnostics { get; set; } = new();
        public List<UnbalancedVoucherDiag> TopUnbalancedVouchers { get; set; } = new();
        public List<OrphanLedgerDiag> OrphanLedgers { get; set; } = new();
    }

    /// <summary>
    /// Lightweight financial health check — runs 5 aggregate queries, no full report generation.
    /// Returns Green / Yellow / Red for Trial Balance and Balance Sheet independently,
    /// with smart classification of the Balance Sheet difference.
    /// </summary>
    public class FinancialHealthService
    {
        private readonly AppDbContext _db;

        public FinancialHealthService(AppDbContext db)
        {
            _db = db;
        }

        /// <param name="fyStart">Start of the financial year (for P&L net profit calculation).</param>
        /// <param name="asOfDate">Point-in-time cutoff for all calculations.</param>
        public async Task<FinancialHealthResult> ComputeAsync(Guid orgId, DateTime fyStart, DateTime asOfDate)
        {
            var result = new FinancialHealthResult { GeneratedAt = DateTimeOffset.UtcNow };

            // ── Load group classification once ──
            var groups = await _db.AccountingGroups.IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            if (!groups.Any())
            {
                result.OverallStatus = HealthStatus.Red;
                result.Issues.Add("No accounting groups found. Run Master Sync first.");
                return result;
            }

            var incomeNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Income", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name).ToList();

            var expenseNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Expenditure", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name).ToList();

            var assetNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Assets", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name).ToList();

            var liabilityNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Liabilities", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name).ToList();

            // ── Query 1: Trial Balance — total debit MUST equal total credit ──
            // This is the most fundamental accounting invariant.
            // If it fails, ALL reports derived from this data are wrong.
            var tbTotals = await _db.LedgerEntries.IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate <= asOfDate)
                .GroupBy(e => 1)
                .Select(g => new { Debit = g.Sum(e => e.DebitAmount), Credit = g.Sum(e => e.CreditAmount) })
                .FirstOrDefaultAsync();

            result.TBTotalDebit = tbTotals?.Debit ?? 0;
            result.TBTotalCredit = tbTotals?.Credit ?? 0;
            result.TBDifference = Math.Abs(result.TBTotalDebit - result.TBTotalCredit);
            result.TBStatus = result.TBDifference < 0.01m ? HealthStatus.Green : HealthStatus.Red;

            if (result.TBStatus == HealthStatus.Red)
            {
                result.Issues.Add(
                    $"Trial Balance imbalance of {result.TBDifference:N2}. " +
                    "This means at least one voucher has missing or mismatched ledger entries.");
            }

            // ── Query 2: P&L net profit for the current financial year ──
            // Income credit minus debit = net income amount
            // Expense debit minus credit = net expense amount
            // NetProfit = TotalIncome - TotalExpense
            var incomeTotal = incomeNames.Any()
                ? await _db.LedgerEntries.IgnoreQueryFilters()
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate >= fyStart && e.Voucher.VoucherDate <= asOfDate
                             && incomeNames.Contains(e.LedgerGroup))
                    .SumAsync(e => e.CreditAmount - e.DebitAmount)
                : 0;

            var expenseTotal = expenseNames.Any()
                ? await _db.LedgerEntries.IgnoreQueryFilters()
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate >= fyStart && e.Voucher.VoucherDate <= asOfDate
                             && expenseNames.Contains(e.LedgerGroup))
                    .SumAsync(e => e.DebitAmount - e.CreditAmount)
                : 0;

            result.NetProfit = incomeTotal - expenseTotal;

            // ── Queries 3+4: Balance Sheet ──
            // Assets = OpeningBalance(asset ledgers) + net debit movements on asset ledgers
            // Liabilities = |OpeningBalance(liability ledgers)| + net credit movements on liability ledgers
            var assetOpening = assetNames.Any()
                ? await _db.Ledgers.IgnoreQueryFilters()
                    .Where(l => l.OrganizationId == orgId && !l.IsDeleted && l.IsActive
                             && assetNames.Contains(l.ParentGroup))
                    .SumAsync(l => l.OpeningBalance)
                : 0;

            var assetMovements = assetNames.Any()
                ? await _db.LedgerEntries.IgnoreQueryFilters()
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate <= asOfDate
                             && assetNames.Contains(e.LedgerGroup))
                    .SumAsync(e => e.DebitAmount - e.CreditAmount)
                : 0;

            var liabilityOpening = liabilityNames.Any()
                ? await _db.Ledgers.IgnoreQueryFilters()
                    .Where(l => l.OrganizationId == orgId && !l.IsDeleted && l.IsActive
                             && liabilityNames.Contains(l.ParentGroup))
                    .SumAsync(l => l.OpeningBalance)
                : 0;

            var liabilityMovements = liabilityNames.Any()
                ? await _db.LedgerEntries.IgnoreQueryFilters()
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate <= asOfDate
                             && liabilityNames.Contains(e.LedgerGroup))
                    .SumAsync(e => e.CreditAmount - e.DebitAmount)
                : 0;

            // Tally opening balances: assets = positive, liabilities = negative (stored as negative)
            result.BSAssets = assetOpening + assetMovements;
            result.BSLiabilities = Math.Abs(liabilityOpening) + liabilityMovements;
            result.BSDifference = result.BSAssets - result.BSLiabilities;

            // ── Smart difference classification ──
            // Three possible states:
            //   1. Difference ≈ 0          → Balanced (Green)
            //   2. Difference ≈ NetProfit  → Unclosed P&L (Yellow) — normal before year-end closing
            //   3. Anything else           → Data issue (Red)
            if (Math.Abs(result.BSDifference) < 0.01m)
            {
                result.BSStatus = HealthStatus.Green;
                result.BSDifferenceLabel = "Balanced";
            }
            else if (Math.Abs(Math.Abs(result.BSDifference) - Math.Abs(result.NetProfit)) < 1.0m)
            {
                // Tolerance of 1.00 because rounding across many ledgers can accumulate small drift
                result.BSStatus = HealthStatus.Yellow;
                result.BSDifferenceLabel = result.NetProfit >= 0
                    ? "Net Profit not closed to Capital — expected before year-end"
                    : "Net Loss not closed to Capital — expected before year-end";
            }
            else
            {
                result.BSStatus = HealthStatus.Red;
                result.BSDifferenceLabel = $"Unexplained difference of {Math.Abs(result.BSDifference):N2} — check sync completeness";
                result.Issues.Add(
                    $"Balance Sheet difference ({result.BSDifference:N2}) does not match Net Profit ({result.NetProfit:N2}). " +
                    "Likely cause: missing ledger entries, incorrect group mapping, or incomplete sync.");
            }

            // ── Overall status: worst of the two ──
            result.OverallStatus = result.TBStatus == HealthStatus.Red || result.BSStatus == HealthStatus.Red
                ? HealthStatus.Red
                : result.BSStatus == HealthStatus.Yellow
                    ? HealthStatus.Yellow
                    : HealthStatus.Green;

            // ── Drill-down diagnostics — only run when something is wrong ──
            if (result.OverallStatus != HealthStatus.Green)
            {
                // Diagnostic 1: Top 10 unbalanced vouchers
                // A correctly posted double-entry voucher must have SUM(Debit) == SUM(Credit).
                var unbalanced = await _db.LedgerEntries.IgnoreQueryFilters()
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate <= asOfDate)
                    .GroupBy(e => new
                    {
                        e.Voucher.Id,
                        e.Voucher.VoucherNumber,
                        e.Voucher.VoucherDate,
                        e.Voucher.VoucherType
                    })
                    .Select(g => new
                    {
                        g.Key.VoucherNumber,
                        g.Key.VoucherDate,
                        g.Key.VoucherType,
                        Imbalance = g.Sum(e => e.DebitAmount) - g.Sum(e => e.CreditAmount)
                    })
                    .Where(v => Math.Abs(v.Imbalance) > 0.01m)
                    .OrderByDescending(v => Math.Abs(v.Imbalance))
                    .Take(10)
                    .ToListAsync();

                result.TopUnbalancedVouchers = unbalanced.Select(v => new UnbalancedVoucherDiag
                {
                    VoucherNumber = v.VoucherNumber ?? string.Empty,
                    VoucherDate   = v.VoucherDate,
                    VoucherType   = v.VoucherType?.Name ?? string.Empty,
                    Imbalance     = v.Imbalance
                }).ToList();

                if (result.TopUnbalancedVouchers.Any())
                {
                    result.Diagnostics.Add(new DiagnosticItem
                    {
                        Description = $"{result.TopUnbalancedVouchers.Count} voucher(s) have unequal debit/credit entries (top offender: {result.TopUnbalancedVouchers[0].VoucherNumber}, imbalance {result.TopUnbalancedVouchers[0].Imbalance:N2})",
                        Severity    = HealthStatus.Red
                    });
                }

                // Diagnostic 2: Orphan ledgers — ParentGroup exists in Ledger table but has no NatureOfGroup
                // This means the ledger's group was not classified during Master Sync and will be
                // excluded from both Balance Sheet and P&L reports.
                var classifiedGroupNames = groups
                    .Where(g => !string.IsNullOrWhiteSpace(g.NatureOfGroup))
                    .Select(g => g.Name)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var orphans = await _db.Ledgers.IgnoreQueryFilters()
                    .Where(l => l.OrganizationId == orgId && !l.IsDeleted && l.IsActive)
                    .Select(l => new { l.Name, l.ParentGroup })
                    .ToListAsync();

                result.OrphanLedgers = orphans
                    .Where(l => !string.IsNullOrWhiteSpace(l.ParentGroup)
                             && !classifiedGroupNames.Contains(l.ParentGroup))
                    .Take(10)
                    .Select(l => new OrphanLedgerDiag { LedgerName = l.Name, ParentGroup = l.ParentGroup })
                    .ToList();

                if (result.OrphanLedgers.Any())
                {
                    result.Diagnostics.Add(new DiagnosticItem
                    {
                        Description = $"{result.OrphanLedgers.Count} ledger(s) belong to unclassified groups (e.g. '{result.OrphanLedgers[0].LedgerName}' in '{result.OrphanLedgers[0].ParentGroup}'). Run Master Sync to fix.",
                        Severity    = HealthStatus.Yellow
                    });
                }

                // Diagnostic 3: Summary of Trial Balance imbalance, if any
                if (result.TBStatus == HealthStatus.Red)
                {
                    result.Diagnostics.Add(new DiagnosticItem
                    {
                        Description = $"Trial Balance off by {result.TBDifference:N2} — check for incomplete voucher imports or manual entry errors.",
                        Severity    = HealthStatus.Red
                    });
                }
            }

            return result;
        }

        /// <summary>Returns the start of the Indian financial year (April 1) for a given date.</summary>
        public static DateTime IndianFYStart(DateTime date) =>
            date.Month >= 4 ? new DateTime(date.Year, 4, 1) : new DateTime(date.Year - 1, 4, 1);
    }
}
