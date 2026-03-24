using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Acczite20.Models;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherSyncController
    {
        private readonly VoucherSyncChunkScheduler _scheduler;
        private readonly TallyVoucherRequestExecutor _executor;
        private readonly VoucherSyncDbWriter _dbWriter;
        private readonly VoucherSyncProgressAggregator _progress;
        private readonly SyncStateMonitor _syncMonitor;

        public VoucherSyncController(
            VoucherSyncChunkScheduler scheduler,
            TallyVoucherRequestExecutor executor,
            VoucherSyncDbWriter dbWriter,
            VoucherSyncProgressAggregator progress,
            SyncStateMonitor syncMonitor)
        {
            _scheduler = scheduler;
            _executor = executor;
            _dbWriter = dbWriter;
            _progress = progress;
            _syncMonitor = syncMonitor;
        }

        public async Task RunAsync(
            Guid orgId,
            Guid companyId,
            DateTimeOffset from,
            DateTimeOffset to,
            Func<Voucher, CancellationToken, ValueTask<Voucher?>> prepareVoucherAsync,
            Func<DateRange, VoucherChunkExecutionMetrics, CancellationToken, Task> onChunkCompletedAsync,
            CancellationToken ct)
        {
            await foreach (var chunk in _scheduler.GetChunksAsync(from, to, ct))
            {
                var metrics = new VoucherChunkExecutionMetrics();
                _syncMonitor.SetStage(
                    "Streaming vouchers",
                    $"Fetching {chunk.Start:yyyy-MM-dd HH:mm} to {chunk.End:yyyy-MM-dd HH:mm} with a {_scheduler.CurrentWindow.TotalHours:0.#} hour window.",
                    58,
                    true);

                var channel = Channel.CreateBounded<Voucher>(new BoundedChannelOptions(5000)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });

                var writerTask = _dbWriter.RunAsync(channel.Reader, ct);

                try
                {
                    await foreach (var voucher in _executor.ExportVouchersStreamAsync(orgId, companyId, chunk, metrics, ct))
                    {
                        var prepared = await prepareVoucherAsync(voucher, ct);
                        if (prepared == null)
                        {
                            metrics.RejectedCount++;
                            continue;
                        }

                        await channel.Writer.WriteAsync(prepared, ct);
                        metrics.EnqueuedCount++;
                        _progress.RecordFetched(1);
                    }

                    channel.Writer.TryComplete();
                    await writerTask;
                    await onChunkCompletedAsync(chunk, metrics, ct);
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);

                    try
                    {
                        await writerTask;
                    }
                    catch
                    {
                        // Preserve the original producer failure.
                    }

                    throw;
                }
            }
        }
    }
}
