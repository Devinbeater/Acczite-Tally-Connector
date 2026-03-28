using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Acczite20.Models;
using Acczite20.Services;

namespace Acczite20.Services.Sync
{
    public sealed class TallyVoucherRequestExecutor
    {
        private readonly TallyXmlService _tallyService;
        private readonly TallyXmlParser _xmlParser;
        private readonly VoucherSyncChunkScheduler _scheduler;
        private readonly SyncStateMonitor _syncMonitor;

        public TallyVoucherRequestExecutor(
            TallyXmlService tallyService,
            TallyXmlParser xmlParser,
            VoucherSyncChunkScheduler scheduler,
            SyncStateMonitor syncMonitor)
        {
            _tallyService = tallyService;
            _xmlParser = xmlParser;
            _scheduler = scheduler;
            _syncMonitor = syncMonitor;
        }

        // ── Two-pass voucher export (Dual-Layer Control) ──────────────────────────
        //
        // Layer 1: Temporal Coarse Slicing (The Density Guard)
        //   Pass 1 — AccziteVoucherHeaders (scalar fields only, extremely lightweight)
        //     Collect voucher shells into an in-memory dictionary keyed by MASTERID.
        //     → Throw ChunkOverloadedException if count > CoarseDensityLimit (e.g. 500) and
        //       window > MinWindow so the controller can retry with a smaller coarse window.
        //
        // Layer 2: MASTERID Batching (The Heavy Guard)
        //   Pass 2 — AccziteVoucherDetail (ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*)
        //     If Layer 1 passes (num records <= 500), slice the resolved MASTERIDs into chunks
        //     of MaxVouchersPerChunk (e.g. 25). Stream ledger/inventory detail for precisely those
        //     MASTERIDs to prevent Tally from compiling massive internal responses.

        public async IAsyncEnumerable<Voucher> ExportVouchersStreamAsync(
            Guid orgId,
            Guid companyId,
            DateRange range,
            VoucherChunkExecutionMetrics metrics,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            metrics.WindowUsed = _scheduler.CurrentWindow;

            var readerSettings = new XmlReaderSettings
            {
                Async = true,
                CheckCharacters = false,
                IgnoreWhitespace = true,
                CloseInput = false
            };

            // ── Pass 1: Voucher headers (scalar fields only) ─────────────────────
            var headers = new Dictionary<string, Voucher>(StringComparer.OrdinalIgnoreCase);
            long headerBytes = 0;

            await using var headerLease = await _tallyService.OpenCollectionXmlStreamAsync(
                "AccziteVoucherHeaders", range.Start, range.End, true, ct);

            if (headerLease == null)
            {
                metrics.Elapsed = sw.Elapsed;
                _scheduler.Adjust(metrics.Elapsed, 0, 0, failed: true, isSafeMode: _syncMonitor.SyncMode == "Safe");
                throw new InvalidOperationException(
                    $"Tally did not return a header stream for {range}.");
            }

            headerBytes = headerLease.DeclaredSize;

            using (var reader = XmlReader.Create(headerLease.Stream, readerSettings))
            {
                while (await reader.ReadAsync())
                {
                    ct.ThrowIfCancellationRequested();

                    if (reader.NodeType != XmlNodeType.Element ||
                        !string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                        continue;

                    XElement element;
                    try { element = (XElement)XNode.ReadFrom(reader); }
                    catch { metrics.RejectedCount++; continue; }

                    var voucher = _xmlParser.ParseVoucherHeader(element, orgId, companyId);
                    if (voucher != null && !string.IsNullOrEmpty(voucher.TallyMasterId))
                    {
                        headers[voucher.TallyMasterId] = voucher;
                    }
                    else
                    {
                        metrics.RejectedCount++;
                    }
                }
            }

            if (headers.Count == 0)
            {
                _syncMonitor.AddLog($"🔍 Discovery: 0 vouchers found for range {range.Start:yyyy-MM-dd} to {range.End:yyyy-MM-dd}. Tally may have no data for these dates.", "WARNING", "VOUCHERS");
            }
            else
            {
                _syncMonitor.AddLog($"✅ Discovery: {headers.Count} voucher headers resolved for {range.Start:yyyy-MM-dd}.", "DEBUG", "VOUCHERS");
            }

            // Empty chunk — nothing to do; let the scheduler grow the window.
            if (headers.Count == 0)
            {
                sw.Stop();
                metrics.Elapsed = sw.Elapsed;
                _syncMonitor.AddLog(
                    $"[PASS1] 0 vouchers returned by Tally for {range.Start:yyyyMMdd}→{range.End:yyyyMMdd} ({sw.Elapsed.TotalSeconds:0.#}s). " +
                    $"Check: (1) company name in Tally matches '{SessionManager.Instance.TallyCompanyName}', " +
                    $"(2) Tally has vouchers between those dates, " +
                    $"(3) see tally_request_stream.xml for exact XML sent.",
                    "WARNING", "DIAG");
                _scheduler.Adjust(metrics.Elapsed, 0, headerBytes, failed: false, isSafeMode: _syncMonitor.SyncMode == "Safe");
                yield break;
            }

            // ── Layer 1: Density Guard (Coarse Control) ────────────────────────────
            if (headers.Count > _scheduler.CoarseDensityLimit)
            {
                if (_scheduler.CurrentWindow > _scheduler.MinWindow)
                {
                    // Time window can still shrink. Abort before the heavy detail pass.
                    // Controller will retry the same date range with a smaller window.
                    sw.Stop();
                    metrics.Elapsed      = sw.Elapsed;
                    metrics.PayloadBytes = headerBytes;
                    _scheduler.Adjust(metrics.Elapsed, headers.Count, headerBytes, failed: true, isSafeMode: _syncMonitor.SyncMode == "Safe");
                    throw new ChunkOverloadedException(headers.Count, _scheduler.CoarseDensityLimit, _scheduler.CurrentWindow);
                }
                
                // Window is already at minimum (e.g. 1 hour). We must proceed despite coarse density.
                _syncMonitor.AddLog($"Density Guard: {_scheduler.CurrentWindow.TotalHours:0.#}h window has {headers.Count} > limit {_scheduler.CoarseDensityLimit}. Proceeding gracefully via Layer 2.", "WARNING", "THROTTLE");
            }

            // ── Layer 2: ID Batching (Fine Control) ───────────────────────────────
            var failed = true;
            try
            {
                var allIds = headers.Keys.ToList();
                int totalBatches = (int)Math.Ceiling(allIds.Count / (double)_scheduler.MaxVouchersPerChunk);
                int currentBatch = 1;

                foreach (var batch in allIds.Chunk(_scheduler.MaxVouchersPerChunk))
                {
                    ct.ThrowIfCancellationRequested();
                    
                    var idList = string.Join(",", batch);
                    metrics.WindowUsed = _scheduler.CurrentWindow;
                    
                    _syncMonitor.AddLog($"Fetching heavy detail batch {currentBatch++}/{totalBatches} ({batch.Length} vouchers)...", "DEBUG", "TALLY");

                    // Safe to request detail fields because Tally evaluates strictly <= 25 records.
                    // Splitting into 2A (Ledgers) and 2B (Inventory) to reduce peak XML payload per response.
                    
                    // ── Pass 2A: Ledgers ──────────────────────────────────────────
                    await using var ledgerLease = await _tallyService.OpenCollectionXmlStreamAsync(
                        "AccziteVoucherLedgers", range.Start, range.End, true, ct, idList);

                    if (ledgerLease == null)
                        throw new InvalidOperationException($"Tally did not return a ledger stream for {range} batch.");

                    metrics.PayloadBytes += ledgerLease.DeclaredSize;
                    await MergeStreamAsync(ledgerLease.Stream, headers, metrics, ct,
                        (el, v) => { if (!_xmlParser.MergeLedgerEntries(el, v)) { metrics.RejectedCount++; return false; } return true; },
                        readerSettings);

                    // ── Pass 2B: Inventory ────────────────────────────────────────
                    await using var inventoryLease = await _tallyService.OpenCollectionXmlStreamAsync(
                        "AccziteVoucherInventory", range.Start, range.End, true, ct, idList);

                    if (inventoryLease != null)
                    {
                        var noCountMetrics = new VoucherChunkExecutionMetrics();
                        metrics.PayloadBytes += inventoryLease.DeclaredSize;
                        await MergeStreamAsync(inventoryLease.Stream, headers, noCountMetrics, ct,
                            (el, v) => { _xmlParser.MergeInventoryEntries(el, v); return true; },
                            readerSettings);
                    }

                    // Delay between batch sets to keep Tally memory un-spiked
                    if (currentBatch <= totalBatches)
                        await Task.Delay(2000, ct); 
                }

                failed = false;
            }
            finally
            {
                sw.Stop();
                metrics.Elapsed = sw.Elapsed;
                _scheduler.Adjust(metrics.Elapsed, metrics.FetchedCount, metrics.PayloadBytes, failed, isSafeMode: _syncMonitor.SyncMode == "Safe");
            }

            // ── Yield completed vouchers ──────────────────────────────────────────
            // yield return must be outside the try/finally block above.
            foreach (var voucher in headers.Values)
            {
                // Only yield vouchers whose detail was successfully merged.
                // Vouchers with TotalAmount == 0 and no ledger entries were either
                // cancelled (safe to sync) or not present in the detail response (skip).
                if (voucher.LedgerEntries.Count > 0 || voucher.IsCancelled)
                    yield return voucher;
            }
        }

        // ── Shared XML stream → header-dict merge helper ───────────────────────────
        // Iterates a Tally XML stream, extracts VOUCHER elements, resolves their
        // MASTERID against the header dict, and calls the provided merge action.
        // Used by both the 2-pass standard path and the 3-pass fallback.
        private static async Task MergeStreamAsync(
            System.IO.Stream stream,
            Dictionary<string, Voucher> headers,
            VoucherChunkExecutionMetrics metrics,
            CancellationToken ct,
            Func<XElement, Voucher, bool> mergeAction,
            XmlReaderSettings settings)
        {
            using var reader = XmlReader.Create(stream, settings);
            while (await reader.ReadAsync())
            {
                ct.ThrowIfCancellationRequested();
                if (reader.NodeType != XmlNodeType.Element ||
                    !string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                    continue;

                XElement element;
                try { element = (XElement)XNode.ReadFrom(reader); }
                catch { metrics.RejectedCount++; continue; }

                var masterId = element.Attribute("REMOTEID")?.Value
                               ?? element.Element("MASTERID")?.Value
                               ?? element.Descendants()
                                         .FirstOrDefault(x => x.Name.LocalName.Equals(
                                             "MASTERID", StringComparison.OrdinalIgnoreCase)
                                             && !x.HasElements)?.Value
                               ?? string.Empty;

                if (!headers.TryGetValue(masterId, out var voucher))
                {
                    metrics.RejectedCount++;
                    continue;
                }

                if (mergeAction(element, voucher))
                    metrics.FetchedCount++;
                else
                    headers.Remove(masterId); // failed integrity — exclude from yield
            }
        }
    }
}
