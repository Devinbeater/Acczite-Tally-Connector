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
        private readonly ISyncControlService _syncControl;
        private readonly int _batchSize;
        private readonly System.Diagnostics.Stopwatch _runStopwatch;
        private readonly List<Voucher> _vouchers = new();
        private int _batchCounter = 0;
        private bool _countLogged = false;

        public VoucherSyncDbWriter(
            Guid orgId,
            IServiceScopeFactory scopeFactory,
            SyncStateMonitor syncMonitor,
            VoucherSyncProgressAggregator progress,
            System.Diagnostics.Stopwatch runStopwatch,
            IMongoProjector projector,
            ISyncControlService syncControl,
            int batchSize = 500)
        {
            _orgId = orgId;
            _scopeFactory = scopeFactory;
            _syncMonitor = syncMonitor;
            _progress = progress;
            _runStopwatch = runStopwatch;
            _projector = projector;
            _syncControl = syncControl;
            _batchSize = batchSize;
        }

        private void Guard(Guid orgId, Guid runId, CancellationToken ct)
        {
            _syncControl.EnsureOwnership(orgId, runId);
            ct.ThrowIfCancellationRequested();
        }

        public async Task RunAsync(Guid orgId, Guid runId, ChannelReader<Voucher> reader, CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var cache = scope.ServiceProvider.GetRequiredService<MasterDataCache>();
            await cache.InitializeAsync(_orgId);

            var handler = scope.ServiceProvider.GetRequiredService<BulkInsertHandler>();

            await foreach (var voucher in reader.ReadAllAsync(ct))
            {
                _vouchers.Add(voucher);
                if (_vouchers.Count >= _batchSize)
                {
                    _batchCounter++;
                    await FlushAsync(orgId, runId, handler, ct);
                }
            }

            if (_vouchers.Any())
            {
                _batchCounter++;
                await FlushAsync(orgId, runId, handler, ct);
            }
        }

        private async Task FlushAsync(Guid orgId, Guid runId, BulkInsertHandler handler, CancellationToken ct)
        {
            if (!_countLogged && _vouchers.Count > 0)
            {
                // Only log if batch counter is a multiple of 5, or if it's the very first/last batch
                if (_batchCounter == 1 || _batchCounter % 5 == 0)
                {
                    _syncMonitor.AddLog($"Incoming batch: {_vouchers.Count} vouchers. Writing to Database...", "DEBUG", "SQL");
                }
            }

            Guard(orgId, runId, ct);
            _syncControl.UpdateHeartbeat(orgId, runId, $"Saving {_vouchers.Count} vouchers", _progress.Snapshot().Written);

            var vouchersToWrite = _vouchers.ToList();
            _vouchers.Clear();

            var ledgerCount = vouchersToWrite.Sum(v => v.LedgerEntries?.Count ?? 0);
            var inventoryCount = vouchersToWrite.Sum(v => v.InventoryAllocations?.Count ?? 0);
            var billCount = vouchersToWrite.Sum(v => v.BillAllocations?.Count ?? 0);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await handler.BulkInsertVouchersAsync(vouchersToWrite);
            sw.Stop();
 
            Guard(orgId, runId, ct); // 🛡️ Post-I/O Guard

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
