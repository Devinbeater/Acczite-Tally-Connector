using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Acczite20.Models;
using Acczite20.Services;

namespace Acczite20.Services.Sync
{
    /// <summary>
    /// Thrown by TallyVoucherRequestExecutor when a chunk's header pass returns more
    /// vouchers than MaxVouchersPerChunk.  The executor has already called
    /// _scheduler.Adjust(failed:true) to halve the window before throwing.
    /// VoucherSyncController catches this, does NOT advance the cursor, and retries
    /// the same date range with the now-smaller window.
    /// </summary>
    public sealed class ChunkOverloadedException : Exception
    {
        public int VoucherCount { get; }
        public int Limit        { get; }

        public ChunkOverloadedException(int count, int limit, TimeSpan newWindow)
            : base($"Chunk has {count} vouchers (limit {limit}). Window halved to {newWindow.TotalHours:0.#}h — retrying.")
        {
            VoucherCount = count;
            Limit        = limit;
        }
    }

    public sealed class VoucherSyncController
    {
        private readonly VoucherSyncChunkScheduler _scheduler;
        private readonly TallyVoucherRequestExecutor _executor;
        private readonly VoucherSyncDbWriter _dbWriter;
        private readonly VoucherSyncProgressAggregator _progress;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly TallyXmlService _tallyService;
        private readonly ISyncControlService _syncControl;

        public VoucherSyncController(
            VoucherSyncChunkScheduler scheduler,
            TallyVoucherRequestExecutor executor,
            VoucherSyncDbWriter dbWriter,
            VoucherSyncProgressAggregator progress,
            SyncStateMonitor syncMonitor,
            TallyXmlService tallyService,
            ISyncControlService syncControl)
        {
            _scheduler    = scheduler;
            _executor     = executor;
            _dbWriter     = dbWriter;
            _progress     = progress;
            _syncMonitor  = syncMonitor;
            _tallyService = tallyService;
            _syncControl  = syncControl;
        }

        // ── Manual date iteration ────────────────────────────────────────────────
        //
        // Previously we used _scheduler.GetChunksAsync() which pre-yields fixed DateRanges.
        // That makes it impossible to retry a range after a ChunkOverloadedException
        // because the iterator has already advanced past the date.
        //
        // New design: we drive the date cursor manually.
        //   • On success         → advance cursor past the chunk.
        //   • On ChunkOverloaded → do NOT advance cursor; scheduler already halved the
        //     window; next iteration retries the same start date with a smaller window.
        //   • Overload retries   → capped at MaxOverloadRetries so we can't loop forever.

        private const int MaxOverloadRetries = 4;

        // ── Inter-batch delay by mode ───────────────────────────────────────────
        // Auto:       no extra delay (scheduler controls pace)
        // Safe:       user-configured InterBatchDelayMs (floor 500 ms)
        // Aggressive: 500 ms only — minimal gap between chunks
        private int GetInterBatchDelayMs() => _syncMonitor.SyncMode switch
        {
            "Safe"       => Math.Max(500, _syncMonitor.InterBatchDelayMs),
            "Aggressive" => 500,
            _            => 0
        };

        public async Task RunAsync(
            Guid orgId,
            Guid companyId,
            DateTimeOffset from,
            DateTimeOffset to,
            Func<Voucher, CancellationToken, ValueTask<Voucher?>> prepareVoucherAsync,
            Func<DateRange, VoucherChunkExecutionMetrics, CancellationToken, Task> onChunkCompletedAsync,
            CancellationToken ct)
        {
            if (from > to) return;

            var current        = from;
            int overloadRetries = 0;
            int consecutiveFailures = 0;

            while (current <= to)
            {
                ct.ThrowIfCancellationRequested();

                // ── Pause gate ──────────────────────────────────────────────────
                var syncCtx = _syncControl.GetState(orgId);
                if (syncCtx.IsPaused)
                {
                    _syncMonitor.SetStage(
                        "Paused",
                        "Sync is paused. Click Resume to continue.",
                        _syncMonitor.ProgressPercent,
                        false);
                }
                await syncCtx.PauseGate.WaitAsync(ct);
                syncCtx.PauseGate.Release();

                syncCtx.CurrentPhase = "Voucher Sync";

                // ── Preventive Memory Backoff ───────────────────────────────────
                var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
                var memoryMb = currentProcess.PrivateMemorySize64 / 1024 / 1024;
                if (memoryMb > 400)
                {
                    _syncMonitor.AddLog($"⚠️ [PREVENTIVE] High memory ({memoryMb} MB). Throttling batch size and cooling down...", "WARNING", "THROTTLE");
                    _scheduler.ReduceVoucherCapAndWindow(); // Preventively thin the next batch
                    await Task.Delay(2000, ct); // Brief cooldown
                    if (memoryMb > 480) GC.Collect(); // Force GC if dangerously close
                }

                // Build the chunk boundary from the scheduler's current adaptive window.
                var chunkEnd = (current + _scheduler.CurrentWindow).AddTicks(-1);
                if (chunkEnd > to) chunkEnd = to;
                var chunk = new DateRange(current, chunkEnd);

                // Push live metrics to the UI.
                _syncMonitor.LiveWindowHours = _scheduler.CurrentWindow.TotalHours;
                _syncMonitor.LiveRetries     = overloadRetries;

                var metrics = new VoucherChunkExecutionMetrics();
                _syncMonitor.SetStage(
                    "Streaming vouchers",
                    $"Fetching {chunk.Start:yyyy-MM-dd HH:mm} – {chunk.End:yyyy-MM-dd HH:mm} | window {_scheduler.CurrentWindow.TotalHours:0.#}h | mode {_syncMonitor.SyncMode}.",
                    58,
                    true);

                var channel = Channel.CreateBounded<Voucher>(new BoundedChannelOptions(5000)
                {
                    FullMode     = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = true
                });

                var writerTask = _dbWriter.RunAsync(channel.Reader, ct);
                bool overloaded = false;

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

                        if (prepared.AlterId > metrics.MaxAlterId) metrics.MaxAlterId = prepared.AlterId;

                        await channel.Writer.WriteAsync(prepared, ct);
                        metrics.EnqueuedCount++;
                        _progress.RecordFetched(1);
                    }

                    channel.Writer.TryComplete();
                    await writerTask;
                }
                catch (ChunkOverloadedException overloadEx)
                {
                    // Executor halved the window. Drain channel, don't advance cursor.
                    channel.Writer.TryComplete(overloadEx);
                    try { await writerTask; } catch { }

                    overloadRetries++;
                    _syncMonitor.LiveRetries = overloadRetries;

                    // After 2 retries the time-window is at minimum — shrink the voucher cap too.
                    if (overloadRetries >= 2)
                    {
                        _scheduler.ReduceVoucherCapAndWindow();
                        _syncMonitor.AddLog(
                            $"Window heavily scaled after {overloadRetries} overload retries.",
                            "WARNING", "THROTTLE");
                    }

                    if (overloadRetries <= MaxOverloadRetries)
                    {
                        _syncMonitor.AddLog(
                            $"Density Guard triggers ({overloadEx.VoucherCount} > {overloadEx.Limit}). " +
                            $"Retry {overloadRetries}/{MaxOverloadRetries} | window {_scheduler.CurrentWindow.TotalHours:0.#}h (Coarse Cap: {_scheduler.CoarseDensityLimit}).",
                            "WARNING", "THROTTLE");
                        overloaded = true;
                    }
                    else
                    {
                        // Retries exhausted. If circuit is open, execute a hard 60 s cooldown
                        // so Tally fully recovers before the 3-pass fallback fires.
                        if (TallyXmlService.IsCircuitOpen())
                        {
                            _syncMonitor.TallyHealth = "Overloaded";
                            _syncMonitor.AddLog(
                                "Circuit is OPEN and overload retries exhausted — 60 s hard cooldown before 3-pass fallback.",
                                "ERROR", "COOLDOWN");
                            _tallyService.ResetThrottle();
                            await Task.Delay(60_000, ct);
                        }
                        else
                        {
                            _syncMonitor.AddLog(
                                $"Overload retry limit reached (cap={_scheduler.MaxVouchersPerChunk}). 3-pass fallback will activate.",
                                "WARNING", "THROTTLE");
                        }
                        overloadRetries = 0;
                    }
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                    try { await writerTask; } catch { }

                    consecutiveFailures++;
                    
                    bool isTimeoutOrMemory = ex is TaskCanceledException || ex is System.Net.Http.HttpRequestException || ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) || ex.Message.Contains("memory", StringComparison.OrdinalIgnoreCase);

                    if (isTimeoutOrMemory)
                    {
                        _syncMonitor.AddLog($"Chunk failed: {ex.Message}. Consecutive Failures: {consecutiveFailures}", "WARNING", "SCHEDULER");
                        
                        if (consecutiveFailures >= 3)
                        {
                            _syncMonitor.AddLog("Multiple consecutive failures. Activating SAFE MODE.", "ERROR", "SCHEDULER");
                            _scheduler.ActivateSafeMode();
                            consecutiveFailures = 0; 
                        }
                        else
                        {
                            _scheduler.ReduceVoucherCapAndWindow();
                        }
                        overloaded = true; // Retry same boundaries
                    }
                    else
                    {
                        // Structural error or parsing error we cannot auto-recover
                        throw;
                    }
                }

                if (overloaded)
                {
                    // Retry same start with smaller window — do NOT advance cursor.
                    _syncMonitor.AddLog($"Retrying Date Range: {chunk.Start:yyyy-MM-dd HH:mm} to {chunkEnd:yyyy-MM-dd HH:mm}", "INFO", "SCHEDULER");
                    await Task.Yield();
                    continue;
                }

                overloadRetries = 0;
                consecutiveFailures = 0;

                double currentDensity = metrics.FetchedCount / Math.Max(1.0, _scheduler.CurrentWindow.TotalMinutes);
                _syncMonitor.AddLog(
                    $"[METRICS] Window: {_scheduler.CurrentWindow.TotalMinutes}m | Vouchers: {metrics.FetchedCount} | Density: {currentDensity:0.##}v/m | Resp: {metrics.Elapsed.TotalMilliseconds:0}ms | Status: Success | Retries: {overloadRetries}",
                    "DEBUG", "METRICS");
                await onChunkCompletedAsync(chunk, metrics, ct);

                if (chunkEnd >= to) break;
                current = chunkEnd.AddTicks(1);

                // ── Inter-batch delay (Safe / Aggressive modes) ─────────────────
                var delayMs = GetInterBatchDelayMs();
                _syncMonitor.LiveDelayMs = delayMs;
                if (delayMs > 0)
                {
                    _syncMonitor.AddLog(
                        $"[{_syncMonitor.SyncMode}] Waiting {delayMs / 1000.0:0.#}s before next batch.",
                        "INFO", "THROTTLE");
                    await Task.Delay(delayMs, ct);
                }
                else
                {
                    await Task.Yield();
                }
            }
        }
    }
}
