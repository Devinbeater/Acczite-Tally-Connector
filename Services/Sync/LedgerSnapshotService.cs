using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Warehouse;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Sync
{
    public class LedgerSnapshotService
    {
        private readonly AppDbContext _dbContext;

        public LedgerSnapshotService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task UpdateSnapshotsAsync(Guid organizationId, List<FactLedgerEntry> entries)
        {
            if (!entries.Any()) return;

            // Group by Ledger, Year, Month calculation
            var deltas = entries
                .GroupBy(e => new { e.LedgerId, e.VoucherDate.Year, e.VoucherDate.Month })
                .Select(g => new
                {
                    g.Key.LedgerId,
                    g.Key.Year,
                    g.Key.Month,
                    DebitTotal = g.Sum(x => x.Debit),
                    CreditTotal = g.Sum(x => x.Credit)
                }).ToList();

            foreach (var delta in deltas)
            {
                var snapshot = await _dbContext.LedgerBalanceSnapshots
                    .FirstOrDefaultAsync(s => s.OrganizationId == organizationId &&
                                            s.LedgerId == delta.LedgerId &&
                                            s.Year == delta.Year &&
                                            s.Month == delta.Month);

                if (snapshot == null)
                {
                    snapshot = new LedgerBalanceSnapshot
                    {
                        Id = Guid.NewGuid(),
                        OrganizationId = organizationId,
                        LedgerId = delta.LedgerId,
                        Year = delta.Year,
                        Month = delta.Month,
                        DebitTotal = delta.DebitTotal,
                        CreditTotal = delta.CreditTotal,
                        CreatedAt = DateTimeOffset.UtcNow,
                        UpdatedAt = DateTimeOffset.UtcNow
                    };
                    await _dbContext.LedgerBalanceSnapshots.AddAsync(snapshot);
                }
                else
                {
                    snapshot.DebitTotal += delta.DebitTotal;
                    snapshot.CreditTotal += delta.CreditTotal;
                    snapshot.UpdatedAt = DateTimeOffset.UtcNow;
                }
            }

            await _dbContext.SaveChangesAsync();
        }
    }
}
