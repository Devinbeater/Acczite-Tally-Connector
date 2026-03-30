using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Acczite20.Services.Reports
{
    public class PLGroupModel
    {
        public string GroupName  { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public List<PLLedgerModel> Ledgers { get; set; } = new();
    }

    public class PLLedgerModel
    {
        public string LedgerName { get; set; } = string.Empty;
        public decimal Amount    { get; set; }
    }

    public class PandLReportModel
    {
        // Trading Account (AffectsGrossProfit = true)
        public List<PLGroupModel> DirectIncomes    { get; set; } = new();
        public List<PLGroupModel> DirectExpenses   { get; set; } = new();

        // Profit & Loss Account (AffectsGrossProfit = false)
        public List<PLGroupModel> IndirectIncomes  { get; set; } = new();
        public List<PLGroupModel> IndirectExpenses { get; set; } = new();

        public decimal TotalDirectIncome    => DirectIncomes   .Sum(g => g.TotalAmount);
        public decimal TotalDirectExpense   => DirectExpenses  .Sum(g => g.TotalAmount);
        public decimal TotalIndirectIncome  => IndirectIncomes .Sum(g => g.TotalAmount);
        public decimal TotalIndirectExpense => IndirectExpenses.Sum(g => g.TotalAmount);

        public decimal GrossProfit => TotalDirectIncome  - TotalDirectExpense;
        public decimal NetProfit   => GrossProfit + TotalIndirectIncome - TotalIndirectExpense;
        public bool    IsProfit    => NetProfit >= 0;

        public DateTime FromDate { get; set; }
        public DateTime ToDate   { get; set; }
    }

    public class PandLService
    {
        private readonly TrialBalanceService _tbService;

        public PandLService(TrialBalanceService tbService)
        {
            _tbService = tbService;
        }

        public async Task<PandLReportModel> GetPandLReportAsync(Guid orgId, DateTime fromDate, DateTime toDate)
        {
            var from = new DateTimeOffset(fromDate.Date, TimeSpan.Zero);
            var to   = new DateTimeOffset(toDate.Date.AddDays(1).AddTicks(-1), TimeSpan.Zero);

            // Single call — TB already uses fact tables, no extra DB hit here.
            var tb = await _tbService.GenerateTrialBalanceAsync(orgId, fromDate: from, asOfDate: to);

            return new PandLReportModel
            {
                FromDate         = fromDate,
                ToDate           = toDate,
                DirectIncomes    = tb.Incomes .Where(g =>  g.AffectsGrossProfit).Select(g => ToPlGroup(g, isIncome: true )).Where(g => g.TotalAmount != 0).ToList(),
                DirectExpenses   = tb.Expenses.Where(g =>  g.AffectsGrossProfit).Select(g => ToPlGroup(g, isIncome: false)).Where(g => g.TotalAmount != 0).ToList(),
                IndirectIncomes  = tb.Incomes .Where(g => !g.AffectsGrossProfit).Select(g => ToPlGroup(g, isIncome: true )).Where(g => g.TotalAmount != 0).ToList(),
                IndirectExpenses = tb.Expenses.Where(g => !g.AffectsGrossProfit).Select(g => ToPlGroup(g, isIncome: false)).Where(g => g.TotalAmount != 0).ToList(),
            };
        }

        // ── Helpers ─────────────────────────────────────────────────────────────

        private static PLGroupModel ToPlGroup(TrialBalanceGroup g, bool isIncome)
        {
            var ledgers = CollectLedgers(g, isIncome);
            return new PLGroupModel
            {
                GroupName   = g.GroupName,
                TotalAmount = ledgers.Sum(l => l.Amount),
                Ledgers     = ledgers.OrderByDescending(l => l.Amount).ToList(),
            };
        }

        // Recursively collects all leaf ledgers from a group + its child groups.
        private static List<PLLedgerModel> CollectLedgers(TrialBalanceGroup g, bool isIncome)
        {
            var result = new List<PLLedgerModel>();

            foreach (var l in g.Ledgers)
            {
                // Income ledgers close on the Credit side; expense ledgers on the Debit side.
                var amount = isIncome ? l.CreditBalance : l.DebitBalance;
                if (amount != 0)
                    result.Add(new PLLedgerModel { LedgerName = l.LedgerName, Amount = amount });
            }

            foreach (var child in g.ChildGroups)
                result.AddRange(CollectLedgers(child, isIncome));

            return result;
        }
    }
}
