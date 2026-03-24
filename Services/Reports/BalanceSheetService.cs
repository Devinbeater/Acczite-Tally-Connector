using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Reports
{
    public class BSGroupModel
    {
        public string GroupName { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public List<BSLedgerModel> Ledgers { get; set; } = new();
    }

    public class BSLedgerModel
    {
        public string LedgerName { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    public class BalanceSheetReportModel
    {
        public List<BSGroupModel> Assets { get; set; } = new();
        public List<BSGroupModel> Liabilities { get; set; } = new();
        public decimal TotalAssets => Assets.Sum(a => a.TotalAmount);
        public decimal TotalLiabilities => Liabilities.Sum(l => l.TotalAmount);
        // Assets must equal Liabilities + Equity. Non-zero difference = data gap.
        public decimal Difference => TotalAssets - TotalLiabilities;
        public bool IsBalanced => Math.Abs(Difference) < 0.01m;
        public DateTime AsOfDate { get; set; }
    }

    public class BalanceSheetService
    {
        private readonly AppDbContext _dbContext;

        public BalanceSheetService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task<BalanceSheetReportModel> GetBalanceSheetAsync(Guid orgId, DateTime toDate)
        {
            var report = new BalanceSheetReportModel { AsOfDate = toDate };

            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
                return report;

            // ─── Step 1: Load all groups and classify by NatureOfGroup ───
            // Tally propagates NatureOfGroup to ALL groups — use it, not hardcoded names.
            var groups = await _dbContext.AccountingGroups
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            var assetGroupNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Assets", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var liabilityGroupNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Liabilities", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (assetGroupNames.Count == 0 && liabilityGroupNames.Count == 0)
                return report; // Master sync not done yet

            // ─── Step 2: Load ledger opening balances ───
            // Balance Sheet is cumulative — it MUST include opening balances from Tally.
            // Tally opening balance convention: positive = debit (asset), negative = credit (liability).
            var ledgerOpenings = await _dbContext.Ledgers
                .IgnoreQueryFilters()
                .Where(l => l.OrganizationId == orgId && !l.IsDeleted && l.IsActive)
                .Select(l => new { l.Name, l.ParentGroup, l.OpeningBalance })
                .ToDictionaryAsync(l => l.Name, l => l, StringComparer.OrdinalIgnoreCase);

            // ─── Step 3: Aggregate transaction movements in SQL ───
            // Net movement per ledger = SUM(Debit) - SUM(Credit) across all non-cancelled vouchers up to toDate.
            var movements = await _dbContext.LedgerEntries
                .IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate <= toDate)
                .GroupBy(e => e.LedgerName)
                .Select(g => new
                {
                    LedgerName = g.Key,
                    TotalDebit = g.Sum(e => e.DebitAmount),
                    TotalCredit = g.Sum(e => e.CreditAmount)
                })
                .ToDictionaryAsync(x => x.LedgerName, x => x, StringComparer.OrdinalIgnoreCase);

            // ─── Step 4: Compute closing balance per ledger ───
            // Closing = Opening + (TotalDebit - TotalCredit)
            // Sign determines debit/credit nature — do not force-flip based on group type.
            var ledgerBalances = ledgerOpenings.Values
                .Select(l =>
                {
                    movements.TryGetValue(l.Name, out var mv);
                    decimal netMovement = (mv?.TotalDebit ?? 0) - (mv?.TotalCredit ?? 0);
                    decimal closingBalance = l.OpeningBalance + netMovement;
                    return new
                    {
                        l.Name,
                        l.ParentGroup,
                        ClosingBalance = closingBalance
                    };
                })
                .ToList();

            // ─── Step 5: Classify and group ───
            var assetLedgers = ledgerBalances
                .Where(l => assetGroupNames.Contains(l.ParentGroup))
                .Where(l => l.ClosingBalance != 0);

            var liabilityLedgers = ledgerBalances
                .Where(l => liabilityGroupNames.Contains(l.ParentGroup))
                .Where(l => l.ClosingBalance != 0);

            report.Assets = assetLedgers
                .GroupBy(l => l.ParentGroup)
                .Select(g => new BSGroupModel
                {
                    GroupName = g.Key,
                    // Assets have normal debit balance (positive = debit)
                    TotalAmount = g.Sum(l => l.ClosingBalance),
                    Ledgers = g.Select(l => new BSLedgerModel
                    {
                        LedgerName = l.Name,
                        Amount = l.ClosingBalance
                    }).OrderByDescending(l => Math.Abs(l.Amount)).ToList()
                })
                .OrderByDescending(g => g.TotalAmount)
                .ToList();

            report.Liabilities = liabilityLedgers
                .GroupBy(l => l.ParentGroup)
                .Select(g => new BSGroupModel
                {
                    GroupName = g.Key,
                    // Liabilities have normal credit balance (stored as negative in Tally; show as absolute)
                    TotalAmount = Math.Abs(g.Sum(l => l.ClosingBalance)),
                    Ledgers = g.Select(l => new BSLedgerModel
                    {
                        LedgerName = l.Name,
                        Amount = Math.Abs(l.ClosingBalance)
                    }).OrderByDescending(l => l.Amount).ToList()
                })
                .OrderByDescending(g => g.TotalAmount)
                .ToList();

            return report;
        }
    }
}
