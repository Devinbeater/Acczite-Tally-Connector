using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Reports
{
    public class DaybookRow
    {
        public Guid    VoucherId       { get; set; }
        public DateTime VoucherDate    { get; set; }
        public string  VoucherNumber   { get; set; } = string.Empty;
        public string  VoucherType     { get; set; } = string.Empty;
        public string  PartyLedgerName { get; set; } = string.Empty;
        public string  Narration       { get; set; } = string.Empty;
        public decimal DebitAmount     { get; set; }
        public decimal CreditAmount    { get; set; }
    }

    public class PagedDaybookResult
    {
        public List<DaybookRow> Rows       { get; set; } = new();
        public int  TotalCount             { get; set; }
        public int  Page                   { get; set; }
        public int  PageSize               { get; set; }
        public int  TotalPages             => PageSize > 0 ? (int)Math.Ceiling((double)TotalCount / PageSize) : 0;
        public bool HasPrevious            => Page > 1;
        public bool HasNext                => Page < TotalPages;
        public decimal PeriodTotalDebit    { get; set; }
        public decimal PeriodTotalCredit   { get; set; }
    }

    public class DaybookService
    {
        private readonly AppDbContext _db;

        private static readonly HashSet<string> PartyGroups = new(StringComparer.OrdinalIgnoreCase)
        {
            "Sundry Debtors",
            "Sundry Creditors",
            "Bank Accounts",
            "Bank OD A/c",
            "Cash-in-Hand",
            "Loans & Advances (Asset)",
            "Loans (Liability)",
            "Secured Loans",
            "Unsecured Loans",
        };

        // Groups that should never be chosen as the party in a fallback scenario.
        private static readonly HashSet<string> NonPartyGroups = new(StringComparer.OrdinalIgnoreCase)
        {
            "Duties & Taxes",
            "Direct Expenses",
            "Indirect Expenses",
            "Direct Incomes",
            "Indirect Incomes",
            "Sales Accounts",
            "Purchase Accounts",
        };

        public DaybookService(AppDbContext db)
        {
            _db = db;
        }

        public async Task<PagedDaybookResult> GetPagedAsync(
            Guid orgId,
            DateTime from,
            DateTime to,
            int page              = 1,
            int pageSize          = 100,
            bool skipPeriodTotals = false)
        {
            page     = Math.Max(1, page);
            pageSize = Math.Clamp(pageSize, 10, 500);

            var fromOffset = new DateTimeOffset(from.Date,                           TimeSpan.Zero);
            var toOffset   = new DateTimeOffset(to.Date.AddDays(1).AddTicks(-1),     TimeSpan.Zero);

            // ── Query 1: Count + paged FactVouchers ─────────────────────────────
            // LEFT JOIN to VoucherTypes (transactional master).
            // Narration via correlated scalar subquery — safe even if a voucher somehow
            // has multiple FactNarration rows (unique index prevents this going forward).
            var baseQuery =
                from fv in _db.FactVouchers
                join vt in _db.VoucherTypes on fv.VoucherTypeId equals vt.Id into vtGroup
                from vt in vtGroup.DefaultIfEmpty()
                where fv.OrganizationId == orgId
                   && !fv.IsCancelled
                   && !fv.IsOptional
                   && fv.VoucherDate >= fromOffset
                   && fv.VoucherDate <= toOffset
                orderby fv.VoucherDate, fv.VoucherNumber
                select new
                {
                    fv.Id,
                    fv.VoucherDate,
                    VoucherNumber = fv.VoucherNumber ?? string.Empty,
                    TypeName      = vt != null ? vt.Name : "Journal",
                    Narration     = _db.FactNarrations
                                       .Where(fn => fn.VoucherId == fv.Id)
                                       .Select(fn => fn.Narration)
                                       .FirstOrDefault() ?? string.Empty,
                };

            var totalCount = await baseQuery.CountAsync();
            var skip       = (page - 1) * pageSize;
            var pageVouchers = await baseQuery.Skip(skip).Take(pageSize).ToListAsync();

            if (pageVouchers.Count == 0)
            {
                return new PagedDaybookResult
                {
                    TotalCount = totalCount,
                    Page       = page,
                    PageSize   = pageSize,
                };
            }

            var voucherIds = pageVouchers.Select(v => v.Id).ToList();

            // ── Query 2: Debit/Credit aggregates for this page ───────────────────
            // FactLedgerEntry.LedgerId references Ledgers.Id (transactional).
            // Join to Ledgers to get LedgerName + ParentGroup for party detection.
            var ledgerRows = await (
                from fle in _db.FactLedgerEntries
                join l   in _db.Ledgers on fle.LedgerId equals l.Id into lGroup
                from l   in lGroup.DefaultIfEmpty()
                where voucherIds.Contains(fle.VoucherId)
                select new
                {
                    fle.VoucherId,
                    fle.Debit,
                    fle.Credit,
                    LedgerName  = l != null ? l.Name        : string.Empty,
                    ParentGroup = l != null ? l.ParentGroup : string.Empty,
                }
            ).ToListAsync();

            // Group in memory — tiny dataset per page (pageSize × avg 2–5 entries)
            var aggByVoucher = ledgerRows
                .GroupBy(r => r.VoucherId)
                .ToDictionary(
                    g => g.Key,
                    g =>
                    {
                        var totalDebit  = g.Sum(r => r.Debit);
                        var totalCredit = g.Sum(r => r.Credit);

                        // Party: prefer a known party-group ledger; fallback to the
                        // largest-amount entry that isn't a tax/expense/income group.
                        var party =
                            g.FirstOrDefault(r => PartyGroups.Contains(r.ParentGroup))
                            ?.LedgerName
                            ?? g.Where(r => !NonPartyGroups.Contains(r.ParentGroup))
                               .OrderByDescending(r => r.Debit + r.Credit)
                               .Select(r => r.LedgerName)
                               .FirstOrDefault()
                            ?? string.Empty;

                        return (totalDebit, totalCredit, party);
                    });

            // ── Merge and return ─────────────────────────────────────────────────
            var rows = pageVouchers.Select(v =>
            {
                var (dr, cr, party) = aggByVoucher.TryGetValue(v.Id, out var agg)
                    ? agg
                    : (0m, 0m, string.Empty);

                return new DaybookRow
                {
                    VoucherId       = v.Id,
                    VoucherDate     = v.VoucherDate.DateTime,
                    VoucherNumber   = v.VoucherNumber,
                    VoucherType     = v.TypeName,
                    Narration       = v.Narration,
                    DebitAmount     = dr,
                    CreditAmount    = cr,
                    PartyLedgerName = party,
                };
            }).ToList();

            // Period totals (across all pages, not just this page).
            // Uses denormalized VoucherDate on FactLedgerEntry to avoid a correlated subquery.
            decimal periodDebit = 0, periodCredit = 0;
            if (!skipPeriodTotals)
            {
                var periodTotals = await (
                    from fle in _db.FactLedgerEntries
                    join fv  in _db.FactVouchers on fle.VoucherId equals fv.Id
                    where fle.OrganizationId == orgId
                       && fle.VoucherDate    >= fromOffset
                       && fle.VoucherDate    <= toOffset
                       && !fv.IsCancelled
                       && !fv.IsOptional
                    group fle by 1 into g
                    select new { TotalDebit = g.Sum(r => r.Debit), TotalCredit = g.Sum(r => r.Credit) }
                ).FirstOrDefaultAsync();

                periodDebit  = periodTotals?.TotalDebit  ?? 0;
                periodCredit = periodTotals?.TotalCredit ?? 0;
            }

            return new PagedDaybookResult
            {
                Rows              = rows,
                TotalCount        = totalCount,
                Page              = page,
                PageSize          = pageSize,
                PeriodTotalDebit  = periodDebit,
                PeriodTotalCredit = periodCredit,
            };
        }
    }
}
