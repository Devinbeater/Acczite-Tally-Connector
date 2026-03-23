using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Reports
{
    public class PLGroupModel
    {
        public string GroupName { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public List<PLLedgerModel> Ledgers { get; set; } = new();
    }

    public class PLLedgerModel
    {
        public string LedgerName { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    public class PandLReportModel
    {
        public List<PLGroupModel> Incomes { get; set; } = new();
        public List<PLGroupModel> Expenses { get; set; } = new();
        public decimal TotalIncome => Incomes.Sum(i => i.TotalAmount);
        public decimal TotalExpense => Expenses.Sum(e => e.TotalAmount);
        public decimal NetProfit => TotalIncome - TotalExpense;
    }

    public class PandLService
    {
        private readonly AppDbContext _dbContext;

        public PandLService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task<PandLReportModel> GetPandLReportAsync(Guid orgId, DateTime fromDate, DateTime toDate)
        {
            var report = new PandLReportModel();

            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
            {
                return report;
            }

            // Fetch all ledger entries in range
            var entries = await _dbContext.LedgerEntries
                .IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted &&
                           e.Voucher.VoucherDate >= fromDate && e.Voucher.VoucherDate <= toDate)
                .Include(e => e.Voucher)
                .ToListAsync();

            var groups = await _dbContext.AccountingGroups
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            var incomeGroups = new HashSet<string> { "Sales Accounts", "Direct Incomes", "Indirect Incomes" };
            var expenseGroups = new HashSet<string> { "Purchase Accounts", "Direct Expenses", "Indirect Expenses" };

            // Find all sub-groups for each primary group
            var allIncomes = entries.Where(e => IsInGroup(e.LedgerGroup, incomeGroups, groups));
            var allExpenses = entries.Where(e => IsInGroup(e.LedgerGroup, expenseGroups, groups));

            report.Incomes = allIncomes.GroupBy(e => e.LedgerGroup)
                .Select(g => new PLGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => (e.CreditAmount - e.DebitAmount)), // Credit is income
                    Ledgers = g.GroupBy(e => e.LedgerName)
                               .Select(lg => new PLLedgerModel { LedgerName = lg.Key, Amount = lg.Sum(e => (e.CreditAmount - e.DebitAmount)) })
                               .ToList()
                }).ToList();

            report.Expenses = allExpenses.GroupBy(e => e.LedgerGroup)
                .Select(g => new PLGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => (e.DebitAmount - e.CreditAmount)), // Debit is expense
                    Ledgers = g.GroupBy(e => e.LedgerName)
                               .Select(lg => new PLLedgerModel { LedgerName = lg.Key, Amount = lg.Sum(e => (e.DebitAmount - e.CreditAmount)) })
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
