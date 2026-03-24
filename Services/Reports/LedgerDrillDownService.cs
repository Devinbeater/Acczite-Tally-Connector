using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Acczite20.Data;

namespace Acczite20.Services.Reports
{
    public class LedgerEntryRow
    {
        public DateTimeOffset Date       { get; set; }
        public string VoucherNumber      { get; set; } = string.Empty;
        public string VoucherType        { get; set; } = string.Empty;
        public string Narration          { get; set; } = string.Empty;
        public decimal Debit             { get; set; }
        public decimal Credit            { get; set; }
        public decimal RunningBalance    { get; set; }
        public Guid VoucherId            { get; set; }
    }

    public class LedgerDrillDownResult
    {
        public string LedgerName               { get; set; } = string.Empty;
        public DateTimeOffset PeriodFrom        { get; set; }
        public DateTimeOffset PeriodTo          { get; set; }

        /// <summary>Opening balance at the start of the requested period.</summary>
        public decimal OpeningBalance           { get; set; }

        /// <summary>Balance at the START of the current page (opening + all prior-page entries).</summary>
        public decimal RunningOpeningBalance    { get; set; }

        public List<LedgerEntryRow> Rows        { get; set; } = new();
        public int TotalCount                   { get; set; }
        public int Page                         { get; set; }
        public int PageSize                     { get; set; }

        public int TotalPages  => TotalCount == 0 ? 1 : (int)Math.Ceiling(TotalCount / (double)PageSize);
        public bool HasPrevious => Page > 1;
        public bool HasNext     => Page < TotalPages;

        public int FirstRowNumber => TotalCount == 0 ? 0 : ((Page - 1) * PageSize) + 1;
        public int LastRowNumber  => Math.Min(Page * PageSize, TotalCount);
    }

    public class LedgerDrillDownService
    {
        private readonly AppDbContext _context;
        private const int DefaultPageSize = 100;

        public LedgerDrillDownService(AppDbContext context) => _context = context;

        public async Task<LedgerDrillDownResult> GetAsync(
            Guid orgId,
            string ledgerName,
            DateTimeOffset from,
            DateTimeOffset to,
            int page     = 1,
            int pageSize = DefaultPageSize)
        {
            page     = Math.Max(1, page);
            pageSize = Math.Clamp(pageSize, 10, 500);
            int skip = (page - 1) * pageSize;

            // ── Opening Balance ────────────────────────────────────────────────────
            // Master opening from Tally + net of all entries that pre-date the period.
            var masterOpening = await _context.Ledgers.IgnoreQueryFilters()
                .Where(l => l.OrganizationId == orgId && !l.IsDeleted
                         && l.Name == ledgerName)
                .Select(l => (decimal?)l.OpeningBalance)
                .FirstOrDefaultAsync() ?? 0m;

            var priorNet = await _context.LedgerEntries.IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && e.LedgerName == ledgerName
                         && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate < from)
                .SumAsync(e => (decimal?)(e.DebitAmount - e.CreditAmount)) ?? 0m;

            decimal openingBalance = masterOpening + priorNet;

            // ── Base query (ordered, filtered) ─────────────────────────────────────
            // Flat projection — no Include, no N+1. EF Core compiles this as a JOIN.
            var baseQuery = _context.LedgerEntries.IgnoreQueryFilters()
                .Where(e => e.OrganizationId == orgId && !e.IsDeleted
                         && e.LedgerName == ledgerName
                         && !e.Voucher.IsDeleted && !e.Voucher.IsCancelled && !e.Voucher.IsOptional
                         && e.Voucher.VoucherDate >= from && e.Voucher.VoucherDate <= to)
                .OrderBy(e => e.Voucher.VoucherDate)
                .ThenBy(e => e.Voucher.VoucherNumber)
                .ThenBy(e => e.Id); // stable tie-break

            var totalCount = await baseQuery.CountAsync();

            // ── Running balance at the start of this page ──────────────────────────
            // Required for correct running balance on pages > 1.
            // One extra aggregation query — necessary, not avoidable with pagination.
            decimal runningOpening = openingBalance;
            if (skip > 0)
            {
                var priorPagesNet = await baseQuery
                    .Take(skip)
                    .SumAsync(e => (decimal?)(e.DebitAmount - e.CreditAmount)) ?? 0m;
                runningOpening = openingBalance + priorPagesNet;
            }

            // ── Current page rows ──────────────────────────────────────────────────
            var rows = await baseQuery
                .Skip(skip)
                .Take(pageSize)
                .Select(e => new LedgerEntryRow
                {
                    Date          = e.Voucher.VoucherDate,
                    VoucherNumber = e.Voucher.VoucherNumber ?? string.Empty,
                    VoucherType   = e.Voucher.VoucherType != null ? e.Voucher.VoucherType.Name : string.Empty,
                    Narration     = e.Voucher.Narration ?? string.Empty,
                    Debit         = e.DebitAmount,
                    Credit        = e.CreditAmount,
                    VoucherId     = e.VoucherId
                })
                .ToListAsync();

            // ── Compute running balance in-process (ordered, deterministic) ────────
            var running = runningOpening;
            foreach (var row in rows)
            {
                running += row.Debit - row.Credit;
                row.RunningBalance = running;
            }

            return new LedgerDrillDownResult
            {
                LedgerName            = ledgerName,
                PeriodFrom            = from,
                PeriodTo              = to,
                OpeningBalance        = openingBalance,
                RunningOpeningBalance = runningOpening,
                Rows                  = rows,
                TotalCount            = totalCount,
                Page                  = page,
                PageSize              = pageSize
            };
        }
    }
}
