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
        public decimal Difference => TotalAssets - TotalLiabilities;
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
            var report = new BalanceSheetReportModel();

            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
            {
                return report;
            }

            // Fetch all ledger entry balances up to toDate
            var entries = await _dbContext.LedgerEntries
                .IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted &&
                           e.Voucher.VoucherDate <= toDate)
                .Include(e => e.Voucher)
                .ToListAsync();

            var groups = await _dbContext.AccountingGroups
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            var assetGroups = new HashSet<string> { "Current Assets", "Fixed Assets", "Investments", "Suspense Account" };
            var liabilityGroups = new HashSet<string> { "Current Liabilities", "Loans (Liability)", "Capital Account", "Reserves & Surplus" };

            var allAssets = entries.Where(e => IsInGroup(e.LedgerGroup, assetGroups, groups));
            var allLiabilities = entries.Where(e => IsInGroup(e.LedgerGroup, liabilityGroups, groups));

            report.Assets = allAssets.GroupBy(e => e.LedgerGroup)
                .Select(g => new BSGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => e.DebitAmount - e.CreditAmount),
                    Ledgers = g.GroupBy(e => e.LedgerName)
                               .Select(lg => new BSLedgerModel { LedgerName = lg.Key, Amount = lg.Sum(e => e.DebitAmount - e.CreditAmount) })
                               .ToList()
                }).ToList();

            report.Liabilities = allLiabilities.GroupBy(e => e.LedgerGroup)
                .Select(g => new BSGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => e.CreditAmount - e.DebitAmount), // Liabilities are usually credit
                    Ledgers = g.GroupBy(e => e.LedgerName)
                               .Select(lg => new BSLedgerModel { LedgerName = lg.Key, Amount = lg.Sum(e => e.CreditAmount - e.DebitAmount) })
                               .ToList()
                }).ToList();

            return report;
        }

        private bool IsInGroup(string groupName, HashSet<string> targetGroups, List<AccountingGroup> allGroups)
        {
            if (targetGroups.Contains(groupName)) return true;

            var current = allGroups.FirstOrDefault(g => g.Name == groupName);
            if (current == null || string.IsNullOrEmpty(current.Parent) || current.Parent == groupName) return false;

            return IsInGroup(current.Parent, targetGroups, allGroups);
        }
    }
}
