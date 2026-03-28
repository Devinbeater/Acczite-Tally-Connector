using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Acczite20.Infrastructure;
using Acczite20.Models;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherSyncDbWriter
    {
        private readonly Guid _orgId;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly VoucherSyncProgressAggregator _progress;
        private readonly IMongoProjector _projector;
        private readonly int _batchSize;
        private readonly System.Diagnostics.Stopwatch _runStopwatch;

        public VoucherSyncDbWriter(
            Guid orgId,
            IServiceScopeFactory scopeFactory,
            SyncStateMonitor syncMonitor,
            VoucherSyncProgressAggregator progress,
            System.Diagnostics.Stopwatch runStopwatch,
            IMongoProjector projector,
            int batchSize = 500)
        {
            _orgId = orgId;
            _scopeFactory = scopeFactory;
            _syncMonitor = syncMonitor;
            _progress = progress;
            _runStopwatch = runStopwatch;
            _projector = projector;
            _batchSize = batchSize;
        }

        public async Task RunAsync(ChannelReader<Voucher> reader, CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var cache = scope.ServiceProvider.GetRequiredService<MasterDataCache>();
            await cache.InitializeAsync(_orgId);

            var handler = scope.ServiceProvider.GetRequiredService<BulkInsertHandler>();
            var batch = new List<Voucher>(_batchSize);

            await foreach (var voucher in reader.ReadAllAsync(ct))
            {
                batch.Add(voucher);
                if (batch.Count >= _batchSize)
                {
                    await FlushAsync(handler, batch, ct);
                }
            }

            if (batch.Count > 0)
            {
                await FlushAsync(handler, batch, ct);
            }
        }

        private async Task FlushAsync(BulkInsertHandler handler, List<Voucher> batch, CancellationToken ct)
        {
            var vouchersToWrite = batch.ToList();
            batch.Clear();

            var ledgerCount = vouchersToWrite.Sum(v => v.LedgerEntries?.Count ?? 0);
            var inventoryCount = vouchersToWrite.Sum(v => v.InventoryAllocations?.Count ?? 0);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await handler.BulkInsertVouchersAsync(vouchersToWrite);
            sw.Stop();

            // Project to Mongo (Decoupled & Eventually Consistent)
            foreach (var v in vouchersToWrite)
            {
                _projector.Project("vouchers", ToBsonDocument(v));
            }

            _progress.RecordWritten(vouchersToWrite.Count);
            _syncMonitor.RecordInsertedBatch(vouchersToWrite.Count, ledgerCount, inventoryCount, _runStopwatch.Elapsed.TotalSeconds);

            var snapshot = _progress.Snapshot();
            _syncMonitor.SetStage(
                "Writing vouchers",
                $"Fetched {snapshot.Fetched:N0}, saved {snapshot.Written:N0}. Last DB batch {vouchersToWrite.Count:N0} vouchers in {sw.Elapsed.TotalMilliseconds:N0} ms.",
                72,
                false);

            if (ct.IsCancellationRequested)
            {
                ct.ThrowIfCancellationRequested();
            }
        }
        private static MongoDB.Bson.BsonDocument ToBsonDocument(Voucher v)
        {
            var doc = new MongoDB.Bson.BsonDocument
            {
                { "organizationId", v.OrganizationId.ToString() },
                { "tallyMasterId", v.TallyMasterId ?? string.Empty },
                { "voucherNumber", v.VoucherNumber ?? string.Empty },
                { "date", v.VoucherDate.ToString("yyyy-MM-dd") },
                { "type", v.VoucherType?.Name ?? "Journal" },
                { "amount", (double)Math.Abs(v.LedgerEntries.Where(le => le.DebitAmount > 0).Sum(le => le.DebitAmount)) },
                { "party", "Unknown" },
                { "isCancelled", v.IsCancelled },
                { "updatedAt", DateTime.UtcNow.ToString("O") }
            };
            return doc;
        }
    }
}
