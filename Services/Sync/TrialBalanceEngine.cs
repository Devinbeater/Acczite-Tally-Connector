using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Acczite20.Services.Sync
{
    public class TrialBalanceRow
    {
        public string Name { get; set; } = string.Empty;
        public string ParentName { get; set; } = string.Empty;
        public decimal OpeningBalance { get; set; }
        public decimal Debits { get; set; }
        public decimal Credits { get; set; }
        public decimal ClosingBalance => OpeningBalance + (Debits - Credits);
        public string Nature { get; set; } = string.Empty; // Assets, Liabilities, Income, Expenses
        public bool IsGroup { get; set; }
    }

    public class TrialBalanceEngine
    {
        private readonly AppDbContext _context;
        private readonly ILogger<TrialBalanceEngine> _logger;

        public TrialBalanceEngine(AppDbContext context, ILogger<TrialBalanceEngine> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task<List<TrialBalanceRow>> GenerateTrialBalanceAsync(Guid organizationId)
        {
            _logger.LogInformation("Generating Trial Balance Roll-up...");

            // 1. Base Aggregation (Ledger Entries)
            // Note: We exclude cancelled/optional vouchers
            var ledgerStats = await _context.LedgerEntries
                .Include(le => le.Voucher)
                .Where(le => le.OrganizationId == organizationId && !le.Voucher.IsCancelled && !le.Voucher.IsOptional)
                .GroupBy(le => le.LedgerName)
                .Select(g => new
                {
                    LedgerName = g.Key,
                    Debits = g.Sum(le => le.DebitAmount),
                    Credits = g.Sum(le => le.CreditAmount)
                })
                .ToDictionaryAsync(x => x.LedgerName, x => x);

            // 2. Fetch Masters
            var ledgers = await _context.Ledgers
                .Where(l => l.OrganizationId == organizationId && !l.IsDeleted)
                .ToListAsync();

            var groups = await _context.AccountingGroups
                .Where(g => g.OrganizationId == organizationId && !g.IsDeleted)
                .ToListAsync();

            var groupMap = groups.ToDictionary(g => g.Name, StringComparer.OrdinalIgnoreCase);

            // 3. Initialize Rows for Ledgers
            var rows = new List<TrialBalanceRow>();
            foreach (var l in ledgers)
            {
                ledgerStats.TryGetValue(l.Name, out var stats);
                decimal sumDr = stats?.Debits ?? 0;
                decimal sumCr = stats?.Credits ?? 0;
                
                // INVARIANT VALIDATION (GAP #5): Opening + Dr - Cr == Closing
                // We use 0.01 tolerance for rounding errors.
                if (Math.Abs(l.OpeningBalance + (sumDr - sumCr) - l.ClosingBalance) > 0.01m)
                {
                    _logger.LogCritical($"INVARIANT BREACH: Ledger '{l.Name}' fails Dr/Cr consistency check. (Op: {l.OpeningBalance} + Dr: {sumDr} - Cr: {sumCr} != Cl: {l.ClosingBalance})");
                    // We log this as a critical audit failure. 
                }

                rows.Add(new TrialBalanceRow
                {
                    Name = l.Name,
                    ParentName = l.ParentGroup,
                    OpeningBalance = l.OpeningBalance,
                    Debits = sumDr,
                    Credits = sumCr,
                    Nature = ResolveNature(l.ParentGroup, groupMap),
                    IsGroup = false
                });
            }

            // 4. Roll-up Engine (Recursive aggregation)
            // We iterate until all groups are populated
            var groupRows = new Dictionary<string, TrialBalanceRow>(StringComparer.OrdinalIgnoreCase);
            
            // Build groups from bottom-up
            foreach (var g in groups)
            {
                groupRows[g.Name] = new TrialBalanceRow
                {
                    Name = g.Name,
                    ParentName = g.Parent,
                    Nature = g.NatureOfGroup,
                    IsGroup = true
                };
            }

            // Aggregate child ledgers into parents
            foreach (var r in rows)
            {
                Accumulate(r, r.ParentName, groupRows);
            }

            // 5. Final Validation
            var primaryTotals = groupRows.Values.Where(r => string.IsNullOrEmpty(r.ParentName)).ToList();
            decimal totalDebit = primaryTotals.Sum(t => t.ClosingBalance > 0 ? t.ClosingBalance : 0);
            decimal totalCredit = primaryTotals.Sum(t => t.ClosingBalance < 0 ? Math.Abs(t.ClosingBalance) : 0);

            _logger.LogInformation($"Trial Balance Validation: Total Debit={totalDebit:N2}, Total Credit={totalCredit:N2}");
            
            if (Math.Abs(totalDebit - totalCredit) > 0.1m)
            {
                _logger.LogWarning("Trial Balance Mismatch detected in local aggregation.");
            }

            return rows.Concat(groupRows.Values).OrderBy(r => r.Name).ToList();
        }

        private void Accumulate(TrialBalanceRow child, string parentName, Dictionary<string, TrialBalanceRow> groupRows)
        {
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            AccumulateRecursive(child, parentName, groupRows, visited);
        }

        private void AccumulateRecursive(TrialBalanceRow child, string parentName, Dictionary<string, TrialBalanceRow> groupRows, HashSet<string> visited)
        {
            if (string.IsNullOrEmpty(parentName) || !groupRows.TryGetValue(parentName, out var parent))
                return;

            // CYCLE PROTECTION (GAP #6)
            if (visited.Contains(parentName))
            {
                _logger.LogCritical($"CRITICAL: Circular reference detected in group hierarchy at '{parentName}'. Aborting roll-up.");
                throw new InvalidOperationException($"Cycle detected in Tally Group Hierarchy: {string.Join(" -> ", visited)} -> {parentName}");
            }
            visited.Add(parentName);

            parent.OpeningBalance += child.OpeningBalance;
            parent.Debits += child.Debits;
            parent.Credits += child.Credits;

            // Recurse up the tree
            AccumulateRecursive(child, parent.ParentName, groupRows, visited);
        }

        private string ResolveNature(string groupName, Dictionary<string, AccountingGroup> groupMap)
        {
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            return ResolveNatureRecursive(groupName, groupMap, visited);
        }

        private string ResolveNatureRecursive(string groupName, Dictionary<string, AccountingGroup> groupMap, HashSet<string> visited)
        {
            if (string.IsNullOrEmpty(groupName)) return "Unknown";
            if (visited.Contains(groupName)) return "Mixed/Loop";
            visited.Add(groupName);

            if (groupMap.TryGetValue(groupName, out var g))
            {
                if (!string.IsNullOrEmpty(g.NatureOfGroup)) return g.NatureOfGroup;
                return ResolveNatureRecursive(g.Parent, groupMap, visited);
            }
            return "Unknown";
        }
    }
}
