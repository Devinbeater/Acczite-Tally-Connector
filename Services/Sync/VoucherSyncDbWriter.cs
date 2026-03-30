using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MongoDB.Bson;
using Acczite20.Infrastructure;
using Acczite20.Models;
using Acczite20.Data;
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
            var billCount = vouchersToWrite.Sum(v => v.BillAllocations?.Count ?? 0);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await handler.BulkInsertVouchersAsync(vouchersToWrite);
            sw.Stop();

            // Project to Mongo (Decoupled & Eventually Consistent)
            foreach (var v in vouchersToWrite)
            {
                _projector.Project("vouchers", ToBsonDocument(v));
            }

            _progress.RecordWritten(vouchersToWrite.Count);
            _syncMonitor.RecordInsertedBatch(vouchersToWrite.Count, ledgerCount, inventoryCount, billCount, _runStopwatch.Elapsed.TotalSeconds);

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
            var entries       = v.LedgerEntries?.ToList() ?? new List<LedgerEntry>();
            var partyEntry    = entries.FirstOrDefault(e => e.IsPartyLedger) ?? entries.FirstOrDefault();
            var partyLedger   = partyEntry?.LedgerName ?? string.Empty;
            var totalDebit    = entries.Sum(e => e.DebitAmount);

            var doc = new MongoDB.Bson.BsonDocument
            {
                { "organizationId",  v.OrganizationId.ToString() },
                { "tallyMasterId",   v.TallyMasterId  ?? string.Empty },
                { "voucherNumber",   v.VoucherNumber  ?? string.Empty },
                { "date",            v.VoucherDate.UtcDateTime },
                { "voucherTypeName", v.VoucherType?.Name ?? "Journal" },
                { "narration",       v.Narration      ?? string.Empty },
                { "totalAmount",     (double)totalDebit },
                { "partyLedgerName", partyLedger },
                { "isCancelled",     v.IsCancelled },
                { "ledgerEntries",   new MongoDB.Bson.BsonArray(entries.Select(e => new MongoDB.Bson.BsonDocument
                {
                    { "ledgerName",  e.LedgerName },
                    { "debit",       (double)e.DebitAmount },
                    { "credit",      (double)e.CreditAmount },
                    { "isParty",     e.IsPartyLedger }
                })) },
                { "inventoryAllocations", new MongoDB.Bson.BsonArray((v.InventoryAllocations ?? new List<InventoryAllocation>()).Select(i => new MongoDB.Bson.BsonDocument
                {
                    { "stockItem",   i.StockItemName },
                    { "qty",         (double)i.ActualQuantity },
                    { "rate",        (double)i.Rate },
                    { "amount",      (double)i.Amount }
                })) },
                { "billAllocations", new MongoDB.Bson.BsonArray((v.BillAllocations ?? new List<BillAllocation>()).Select(b => new MongoDB.Bson.BsonDocument
                {
                    { "billName",    b.BillName },
                    { "billType",    b.BillType },
                    { "amount",      (double)b.Amount }
                })) },
                { "updatedAt",       DateTime.UtcNow }
            };
            return doc;
        }
    }
}
