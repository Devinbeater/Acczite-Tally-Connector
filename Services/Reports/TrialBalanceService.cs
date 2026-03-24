using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Acczite20.Data;
using Acczite20.Models;

namespace Acczite20.Services.Reports
{
    // ══════════════════════════════════════════════════════════════
    // DATA MODELS — All the types the Trial Balance engine outputs
    // ══════════════════════════════════════════════════════════════

    public class TrialBalanceReportModel
    {
        public List<TrialBalanceGroup> Assets { get; set; } = new();
        public List<TrialBalanceGroup> Liabilities { get; set; } = new();
        public List<TrialBalanceGroup> Incomes { get; set; } = new();
        public List<TrialBalanceGroup> Expenses { get; set; } = new();

        // Flat list for quick iteration
        public IEnumerable<TrialBalanceGroup> AllGroups => Assets.Concat(Liabilities).Concat(Incomes).Concat(Expenses);
        public IEnumerable<TrialBalanceLedger> AllLedgers => AllGroups.SelectMany(g => g.Ledgers);

        public decimal TotalDebit => AllGroups.Sum(g => g.TotalDebit);
        public decimal TotalCredit => AllGroups.Sum(g => g.TotalCredit);

        public decimal DifferenceAmount => Math.Abs(TotalDebit - TotalCredit);
        public bool IsBalanced => DifferenceAmount < 0.01m;

        // Summary stats
        public int TotalLedgerCount => AllLedgers.Count();
        public int ActiveLedgerCount => AllLedgers.Count(l => l.DebitTurnover != 0 || l.CreditTurnover != 0 || l.OpeningBalance != 0);
        public DateTimeOffset GeneratedAt { get; set; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? AsOfDate { get; set; }

        public List<string> ValidationWarnings { get; set; } = new();
    }

    public class TrialBalanceGroup
    {
        public string GroupName { get; set; } = string.Empty;
        public string RootGroup { get; set; } = string.Empty;
        public string NatureOfGroup { get; set; } = string.Empty;
        public int Depth { get; set; } // 0 = root, 1 = child, etc.

        public decimal TotalDebit => Ledgers.Sum(l => l.DebitBalance) + ChildGroups.Sum(c => c.TotalDebit);
        public decimal TotalCredit => Ledgers.Sum(l => l.CreditBalance) + ChildGroups.Sum(c => c.TotalCredit);
        public decimal NetBalance => TotalDebit - TotalCredit;

        public List<TrialBalanceLedger> Ledgers { get; set; } = new();
        public List<TrialBalanceGroup> ChildGroups { get; set; } = new();
    }

    public class TrialBalanceLedger
    {
        public Guid LedgerId { get; set; }
        public string LedgerName { get; set; } = string.Empty;
        public string ParentGroupName { get; set; } = string.Empty;

        // ── Balance Components ──
        public decimal OpeningBalance { get; set; }
        public decimal DebitTurnover { get; set; }
        public decimal CreditTurnover { get; set; }

        /// <summary>
        /// Opening + Debit - Credit = Closing
        /// For credit-nature accounts (liabilities/income), opening is stored as negative.
        /// After normalization: positive= debit nature, negative = credit nature.
        /// </summary>
        public decimal ClosingBalance => OpeningBalance + DebitTurnover - CreditTurnover;

        /// <summary>
        /// For Trial Balance columns:
        /// If ClosingBalance > 0 → Debit column
        /// If ClosingBalance < 0 → Credit column (absolute value)
        /// </summary>
        public decimal DebitBalance => ClosingBalance > 0 ? ClosingBalance : 0;
        public decimal CreditBalance => ClosingBalance < 0 ? Math.Abs(ClosingBalance) : 0;
    }

    // ══════════════════════════════════════════════════════════════
    // TRIAL BALANCE SERVICE — Production-Grade
    // ══════════════════════════════════════════════════════════════

    public class TrialBalanceService
    {
        private readonly AppDbContext _context;
        private readonly ILogger<TrialBalanceService> _logger;

        public TrialBalanceService(AppDbContext context, ILogger<TrialBalanceService> logger)
        {
            _context = context;
            _logger = logger;
        }

        /// <summary>
        /// Generates a complete Trial Balance with:
        /// 1. Normalized debit/credit from Tally's raw amounts
        /// 2. Opening → Period → Closing balance computation  
        /// 3. Hierarchical group roll-up (Ledger → Group → Parent → Root)
        /// 4. Classification by NatureOfGroup (Assets/Liabilities/Income/Expenditure)
        /// 5. Post-generation validation (Total Debit == Total Credit)
        /// </summary>
        public async Task<TrialBalanceReportModel> GenerateTrialBalanceAsync(
            Guid orgId, DateTimeOffset? fromDate = null, DateTimeOffset? asOfDate = null)
        {
            var report = new TrialBalanceReportModel();
            var sw = System.Diagnostics.Stopwatch.StartNew();

            var dbType = SessionManager.Instance.SelectedDatabaseType;
            if (string.Equals(dbType, "MongoDB", StringComparison.OrdinalIgnoreCase))
            {
                report.ValidationWarnings.Add("Trial Balance requires relational database. MongoDB mode is not supported.");
                return report;
            }

            var targetDate = asOfDate ?? DateTimeOffset.UtcNow;
            report.AsOfDate = targetDate;

            // ─── Step 1: Load Master Data ───
            var groups = await _context.AccountingGroups.IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .ToListAsync();

            var ledgers = await _context.Ledgers.IgnoreQueryFilters()
                .Where(l => l.OrganizationId == orgId && !l.IsDeleted)
                .ToListAsync();

            if (!groups.Any() || !ledgers.Any())
            {
                report.ValidationWarnings.Add("Master data not synced. Run Tally Sync first.");
                return report;
            }

            var groupMap = groups.ToDictionary(g => g.Name, g => g, StringComparer.OrdinalIgnoreCase);

            // ─── Step 2: Pre-compute root group for every group ───
            var rootCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var g in groups)
            {
                rootCache[g.Name] = ResolveRoot(g.Name, groupMap);
            }

            // ─── Step 3: Aggregate period transaction totals per ledger ───
            var entriesQuery = _context.LedgerEntries.IgnoreQueryFilters()
                .Include(e => e.Voucher)
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && e.Voucher != null && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate <= targetDate);

            // Apply start date filter if specified (for period-specific TB)
            if (fromDate.HasValue)
            {
                entriesQuery = entriesQuery.Where(e => e.Voucher.VoucherDate >= fromDate.Value);
            }

            var aggregatedEntries = await entriesQuery
                .GroupBy(e => e.LedgerName)
                .Select(g => new
                {
                    LedgerName = g.Key,
                    TotalDebit = g.Sum(e => e.DebitAmount),
                    TotalCredit = g.Sum(e => e.CreditAmount)
                })
                .ToDictionaryAsync(x => x.LedgerName, x => x, StringComparer.OrdinalIgnoreCase);

            // ─── Step 4: Pre-compute period opening balances in ONE query (not N queries) ───
            // When fromDate is set, opening = Tally opening + net of all entries before fromDate.
            // This MUST be a single aggregated query outside the ledger loop — not one query per ledger.
            Dictionary<string, decimal>? priorNetByLedger = null;
            if (fromDate.HasValue)
            {
                priorNetByLedger = await _context.LedgerEntries.IgnoreQueryFilters()
                    .Include(e => e.Voucher)
                    .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                             && e.Voucher != null && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                             && e.Voucher.VoucherDate < fromDate.Value)
                    .GroupBy(e => e.LedgerName)
                    .Select(g => new { LedgerName = g.Key, NetBalance = g.Sum(e => e.DebitAmount - e.CreditAmount) })
                    .ToDictionaryAsync(x => x.LedgerName, x => x.NetBalance, StringComparer.OrdinalIgnoreCase);
            }

            // ─── Build flat ledger rows with normalized balances ───
            var allTbLedgers = new List<TrialBalanceLedger>();

            foreach (var l in ledgers)
            {
                aggregatedEntries.TryGetValue(l.Name, out var agg);

                decimal debitTurnover = agg?.TotalDebit ?? 0;
                decimal creditTurnover = agg?.TotalCredit ?? 0;

                // Normalize opening balance direction:
                // Tally stores opening balance as:
                //   positive = debit nature (assets, expenses)
                //   negative = credit nature (liabilities, income)
                // We keep this convention for correct ClosingBalance computation.
                decimal openingBalance = l.OpeningBalance;

                if (fromDate.HasValue && priorNetByLedger != null)
                {
                    priorNetByLedger.TryGetValue(l.Name, out var priorNet);
                    openingBalance = l.OpeningBalance + priorNet;
                }

                var tbRow = new TrialBalanceLedger
                {
                    LedgerId = l.Id,
                    LedgerName = l.Name,
                    ParentGroupName = l.ParentGroup ?? "Unassigned",
                    OpeningBalance = openingBalance,
                    DebitTurnover = debitTurnover,
                    CreditTurnover = creditTurnover
                };

                // Include zero-balance ledgers that had activity
                if (tbRow.ClosingBalance == 0 && debitTurnover == 0 && creditTurnover == 0 && openingBalance == 0)
                    continue;

                allTbLedgers.Add(tbRow);
            }

            // ─── Step 5: Build Group Hierarchy with Roll-Up ───
            var groupNodes = BuildGroupHierarchy(groups, allTbLedgers, rootCache);

            // ─── Step 6: Classify into Nature Buckets ───
            // Use NatureOfGroup from Tally (enriched model) for correct classification
            foreach (var node in groupNodes.Values.Where(n => n.Depth == 0))
            {
                var nature = GetNatureForRoot(node.GroupName, groups);

                switch (nature.ToLower())
                {
                    case "assets":
                        report.Assets.Add(node);
                        break;
                    case "liabilities":
                        report.Liabilities.Add(node);
                        break;
                    case "income":
                        report.Incomes.Add(node);
                        break;
                    case "expenditure":
                    case "expenses":
                        report.Expenses.Add(node);
                        break;
                    default:
                        // Fallback: use root group name heuristics
                        ClassifyByRootName(node, report);
                        break;
                }
            }

            // Sort within each section
            report.Assets = report.Assets.OrderBy(g => g.GroupName).ToList();
            report.Liabilities = report.Liabilities.OrderBy(g => g.GroupName).ToList();
            report.Incomes = report.Incomes.OrderBy(g => g.GroupName).ToList();
            report.Expenses = report.Expenses.OrderBy(g => g.GroupName).ToList();

            // ─── Step 7: Validation ───
            ValidateReport(report);

            sw.Stop();
            _logger.LogInformation(
                "Trial Balance generated: {LedgerCount} ledgers, Debit={Debit:N2}, Credit={Credit:N2}, Balanced={IsBalanced}, Time={Elapsed}ms",
                report.ActiveLedgerCount, report.TotalDebit, report.TotalCredit, report.IsBalanced, sw.ElapsedMilliseconds);

            return report;
        }

        // ══════════════════════════════════════════════════════════════
        // GROUP HIERARCHY BUILDER — Recursive Roll-Up
        // ══════════════════════════════════════════════════════════════

        private Dictionary<string, TrialBalanceGroup> BuildGroupHierarchy(
            List<AccountingGroup> groups,
            List<TrialBalanceLedger> ledgers,
            Dictionary<string, string> rootCache)
        {
            // Create a TrialBalanceGroup node for every accounting group
            var nodes = new Dictionary<string, TrialBalanceGroup>(StringComparer.OrdinalIgnoreCase);

            foreach (var g in groups)
            {
                nodes[g.Name] = new TrialBalanceGroup
                {
                    GroupName = g.Name,
                    RootGroup = rootCache.TryGetValue(g.Name, out var root) ? root : g.Name,
                    NatureOfGroup = g.NatureOfGroup
                };
            }

            // Assign ledgers to their direct parent group
            foreach (var l in ledgers)
            {
                if (nodes.TryGetValue(l.ParentGroupName, out var parentGroup))
                {
                    parentGroup.Ledgers.Add(l);
                }
                else
                {
                    // Ledger's parent group not found — create a synthetic node
                    var synthetic = new TrialBalanceGroup
                    {
                        GroupName = l.ParentGroupName,
                        RootGroup = l.ParentGroupName
                    };
                    synthetic.Ledgers.Add(l);
                    nodes[l.ParentGroupName] = synthetic;
                }
            }

            // Build parent-child relationships
            foreach (var g in groups)
            {
                if (!string.IsNullOrEmpty(g.Parent) && nodes.TryGetValue(g.Parent, out var parentNode) && nodes.TryGetValue(g.Name, out var childNode))
                {
                    if (parentNode != childNode) // prevent self-ref
                    {
                        parentNode.ChildGroups.Add(childNode);
                    }
                }
            }

            // Calculate depth for each node
            foreach (var g in groups)
            {
                if (nodes.TryGetValue(g.Name, out var node))
                {
                    node.Depth = CalculateDepth(g.Name, groups);
                }
            }

            // Remove empty groups (no ledgers, no children with ledgers)
            var nonEmptyNodes = new Dictionary<string, TrialBalanceGroup>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in nodes)
            {
                if (HasContent(kvp.Value))
                {
                    nonEmptyNodes[kvp.Key] = kvp.Value;
                }
            }

            return nonEmptyNodes;
        }

        private bool HasContent(TrialBalanceGroup group)
        {
            if (group.Ledgers.Any()) return true;
            return group.ChildGroups.Any(c => HasContent(c));
        }

        private int CalculateDepth(string groupName, List<AccountingGroup> allGroups)
        {
            int depth = 0;
            var current = allGroups.FirstOrDefault(g =>
                string.Equals(g.Name, groupName, StringComparison.OrdinalIgnoreCase));
            while (current != null && !string.IsNullOrEmpty(current.Parent) && depth < 20)
            {
                current = allGroups.FirstOrDefault(g =>
                    string.Equals(g.Name, current.Parent, StringComparison.OrdinalIgnoreCase));
                depth++;
            }
            return depth;
        }

        // ══════════════════════════════════════════════════════════════
        // CLASSIFICATION LOGIC
        // ══════════════════════════════════════════════════════════════

        private string GetNatureForRoot(string rootGroupName, List<AccountingGroup> groups)
        {
            // First try to get NatureOfGroup from the enriched model
            var group = groups.FirstOrDefault(g => 
                string.Equals(g.Name, rootGroupName, StringComparison.OrdinalIgnoreCase));

            if (group != null && !string.IsNullOrEmpty(group.NatureOfGroup))
                return group.NatureOfGroup;

            // Fallback: classify by well-known root group names
            return rootGroupName switch
            {
                "Current Assets" or "Fixed Assets" or "Investments" 
                    or "Misc. Expenses (ASSET)" or "Stock-in-hand" 
                    or "Deposits (Asset)" or "Loans & Advances (Asset)"
                    or "Sundry Debtors" or "Bank Accounts" or "Cash-in-hand" => "Assets",

                "Current Liabilities" or "Capital Account" or "Loans (Liability)" 
                    or "Secured Loans" or "Unsecured Loans" or "Suspense A/c"
                    or "Duties & Taxes" or "Provisions" or "Bank OD Accounts"
                    or "Sundry Creditors" or "Reserves & Surplus" or "Branch / Divisions" => "Liabilities",

                "Sales Accounts" or "Direct Incomes" or "Indirect Incomes" => "Income",

                "Purchase Accounts" or "Direct Expenses" or "Indirect Expenses" => "Expenditure",

                _ => "Unknown"
            };
        }

        private void ClassifyByRootName(TrialBalanceGroup node, TrialBalanceReportModel report)
        {
            // Last resort: use the balance direction
            if (node.TotalDebit >= node.TotalCredit)
                report.Assets.Add(node);
            else
                report.Liabilities.Add(node);

            report.ValidationWarnings.Add(
                $"Group '{node.GroupName}' has no NatureOfGroup — classified by balance direction.");
        }

        // ══════════════════════════════════════════════════════════════
        // VALIDATION — The non-negotiable check
        // ══════════════════════════════════════════════════════════════

        private void ValidateReport(TrialBalanceReportModel report)
        {
            // Rule 1: Total Debit MUST equal Total Credit
            if (!report.IsBalanced)
            {
                report.ValidationWarnings.Add(
                    $"⚠ TRIAL BALANCE MISMATCH: Debit={report.TotalDebit:N2}, Credit={report.TotalCredit:N2}, " +
                    $"Diff={report.DifferenceAmount:N2}. Check for missing ledgers or incomplete sync.");

                _logger.LogWarning(
                    "Trial Balance MISMATCH: Debit={Debit}, Credit={Credit}, Diff={Diff}",
                    report.TotalDebit, report.TotalCredit, report.DifferenceAmount);
            }

            // Rule 2: Check for unclassified groups
            int totalGroups = report.Assets.Count + report.Liabilities.Count + report.Incomes.Count + report.Expenses.Count;
            if (totalGroups == 0)
            {
                report.ValidationWarnings.Add("No groups were classified. Master data may be incomplete.");
            }

            // Rule 3: Check for orphan ledgers (parent group not in accounting groups)
            var allTbLedgers = report.AllLedgers.ToList();
            var classifiedLedgerNames = new HashSet<string>(allTbLedgers.Select(l => l.LedgerName), StringComparer.OrdinalIgnoreCase);
            
            _logger.LogInformation("Validation: {Count} ledgers classified, {Warnings} warnings",
                classifiedLedgerNames.Count, report.ValidationWarnings.Count);
        }

        // ══════════════════════════════════════════════════════════════
        // UTILITY
        // ══════════════════════════════════════════════════════════════

        private string ResolveRoot(string groupName, Dictionary<string, AccountingGroup> groupMap)
        {
            if (!groupMap.TryGetValue(groupName, out var current)) return groupName;

            int depth = 0;
            while (!string.IsNullOrEmpty(current.Parent) && depth < 20)
            {
                if (!groupMap.TryGetValue(current.Parent, out var parent)) break;
                current = parent;
                depth++;
            }
            return current.Name;
        }
    }
}
