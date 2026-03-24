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
        public DateTime FromDate { get; set; }
        public DateTime ToDate { get; set; }
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
            var report = new PandLReportModel { FromDate = fromDate, ToDate = toDate };

            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
                return report;

            // ─── Step 1: Load accounting groups and classify by NatureOfGroup ───
            // NatureOfGroup is propagated by Tally to every group including sub-groups,
            // so this correctly classifies all user-defined custom groups without hardcoding.
            var groups = await _dbContext.AccountingGroups
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            var incomeGroupNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Income", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var expenseGroupNames = groups
                .Where(g => string.Equals(g.NatureOfGroup, "Expenditure", StringComparison.OrdinalIgnoreCase))
                .Select(g => g.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (incomeGroupNames.Count == 0 && expenseGroupNames.Count == 0)
                return report; // Master sync not done yet

            // ─── Step 2: Aggregate in SQL — never load raw rows into memory ───
            // Group by (LedgerGroup, LedgerName) so the DB does the heavy lifting.
            // Exclude cancelled and optional vouchers — they must not affect P&L.
            var aggregated = await _dbContext.LedgerEntries
                .IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate >= fromDate && e.Voucher.VoucherDate <= toDate)
                .GroupBy(e => new { e.LedgerGroup, e.LedgerName })
                .Select(g => new
                {
                    LedgerGroup = g.Key.LedgerGroup,
                    LedgerName = g.Key.LedgerName,
                    TotalDebit = g.Sum(e => e.DebitAmount),
                    TotalCredit = g.Sum(e => e.CreditAmount)
                })
                .ToListAsync();

            // ─── Step 3: Classify in memory against the resolved group sets ───
            var incomeRows = aggregated.Where(e => incomeGroupNames.Contains(e.LedgerGroup));
            var expenseRows = aggregated.Where(e => expenseGroupNames.Contains(e.LedgerGroup));

            // Income: Credit > Debit = positive income (Credit minus Debit)
            report.Incomes = incomeRows
                .GroupBy(e => e.LedgerGroup)
                .Select(g => new PLGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => e.TotalCredit - e.TotalDebit),
                    Ledgers = g.Select(e => new PLLedgerModel
                    {
                        LedgerName = e.LedgerName,
                        Amount = e.TotalCredit - e.TotalDebit
                    }).OrderByDescending(l => l.Amount).ToList()
                })
                .OrderByDescending(g => g.TotalAmount)
                .ToList();

            // Expenses: Debit > Credit = positive expense (Debit minus Credit)
            report.Expenses = expenseRows
                .GroupBy(e => e.LedgerGroup)
                .Select(g => new PLGroupModel
                {
                    GroupName = g.Key,
                    TotalAmount = g.Sum(e => e.TotalDebit - e.TotalCredit),
                    Ledgers = g.Select(e => new PLLedgerModel
                    {
                        LedgerName = e.LedgerName,
                        Amount = e.TotalDebit - e.TotalCredit
                    }).OrderByDescending(l => l.Amount).ToList()
                })
                .OrderByDescending(g => g.TotalAmount)
                .ToList();

            return report;
        }
    }
}
