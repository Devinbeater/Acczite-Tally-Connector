using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Acczite20.Data;
using Acczite20.Models;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Acczite20.Services.Dashboard
{
    public class DashboardService
    {
        private readonly AppDbContext _context;
        private readonly MongoService _mongoService;

        public DashboardService(AppDbContext context, MongoService mongoService)
        {
            _context = context;
            _mongoService = mongoService;
        }

        public async Task<DashboardStats> GetStatsAsync(Guid orgId, CancellationToken ct = default)
        {
            var dbType = SessionManager.Instance.SelectedDatabaseType;
            if (string.Equals(dbType, "MongoDB", StringComparison.OrdinalIgnoreCase))
            {
                return await GetMongoStatsAsync(orgId, ct);
            }

            if (string.IsNullOrWhiteSpace(dbType))
            {
                return new DashboardStats();
            }

            var dateThreshold = DateTimeOffset.UtcNow.AddDays(-7);
            var today = DateTimeOffset.UtcNow.Date;

            return new DashboardStats
            {
                TotalVouchers = await _context.Vouchers.IgnoreQueryFilters().CountAsync(v => v.OrganizationId == orgId, ct),
                TotalLedgers = await _context.Ledgers.IgnoreQueryFilters().CountAsync(l => l.OrganizationId == orgId, ct),
                StockItemCount = await _context.StockItems.IgnoreQueryFilters().CountAsync(s => s.OrganizationId == orgId, ct),
                SyncedToday = await _context.Vouchers.IgnoreQueryFilters().CountAsync(v => v.OrganizationId == orgId && v.CreatedAt >= today, ct),

                VoucherVolumes = await _context.Vouchers.IgnoreQueryFilters()
                    .Where(v => v.OrganizationId == orgId && v.VoucherDate >= dateThreshold)
                    .GroupBy(v => v.VoucherDate.Date)
                    .Select(g => new DailyVoucherVolume { Date = g.Key, Count = g.Count() })
                    .OrderBy(d => d.Date)
                    .ToListAsync(ct),

                GstDistributions = await _context.GstBreakdowns.IgnoreQueryFilters()
                    .Where(g => g.OrganizationId == orgId)
                    .GroupBy(g => g.TaxType)
                    .Select(g => new GstDistribution { TaxType = g.Key, TotalAmount = g.Sum(x => x.TaxAmount) })
                    .ToListAsync(ct),

                InventoryInsights = await _context.InventoryAllocations.IgnoreQueryFilters()
                    .Where(i => i.OrganizationId == orgId)
                    .GroupBy(i => i.StockItemName)
                    .Select(g => new InventoryInsight { ItemName = g.Key, QuantitySold = g.Sum(x => x.ActualQuantity) })
                    .OrderByDescending(x => x.QuantitySold)
                    .Take(10)
                    .ToListAsync(ct)
            };
        }

        private async Task<DashboardStats> GetMongoStatsAsync(Guid orgId, CancellationToken ct)
        {
            var stats = new DashboardStats();
            var db = _mongoService.GetDatabase();
            var filter = _mongoService.GetOrganizationFilter();

            // 1. Basic counts
            var voucherCol = db.GetCollection<BsonDocument>("vouchers");
            var ledgerCol = db.GetCollection<BsonDocument>("ledgers");
            var stockCol = db.GetCollection<BsonDocument>("stockitems");

            stats.TotalVouchers = (int)await voucherCol.CountDocumentsAsync(filter, cancellationToken: ct);
            stats.TotalLedgers = (int)await ledgerCol.CountDocumentsAsync(filter, cancellationToken: ct);
            stats.StockItemCount = (int)await stockCol.CountDocumentsAsync(filter, cancellationToken: ct);

            // 2. Synced Today
            var todayStr = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd");
            var todayFilter = Builders<BsonDocument>.Filter.And(
                filter,
                Builders<BsonDocument>.Filter.Gte("lastModified", todayStr)
            );
            stats.SyncedToday = (int)await voucherCol.CountDocumentsAsync(todayFilter, cancellationToken: ct);

            // 3. Voucher Volumes (last 7 days)
            var weekAgo = DateTimeOffset.UtcNow.AddDays(-7).ToString("yyyy-MM-dd");
            var weekFilter = Builders<BsonDocument>.Filter.And(
                filter,
                Builders<BsonDocument>.Filter.Gte("voucherDate", weekAgo)
            );

            var volumes = await voucherCol.Find(weekFilter)
                .Project(new BsonDocument { { "voucherDate", 1 } })
                .ToListAsync(ct);

            stats.VoucherVolumes = volumes
                .Select(v => {
                    var dateStr = v.GetValue("voucherDate", "").ToString();
                    DateTime.TryParse(dateStr, out var d);
                    return d.Date;
                })
                .Where(d => d != DateTime.MinValue)
                .GroupBy(d => d)
                .Select(g => new DailyVoucherVolume { Date = g.Key, Count = g.Count() })
                .OrderBy(v => v.Date)
                .ToList();

            // 4. GST Distribution & Inventory Insights
            // For MongoDB, we'll keep these empty or implement simple versions if data exists
            // Usually MongoDB documents have nested arrays for these.
            
            return stats;
        }
    }
}
