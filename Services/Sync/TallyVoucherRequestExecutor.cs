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

        // ── Voucher cap — read from scheduler (dynamically reduced under sustained load) ─
        // VoucherSyncController reduces _scheduler.MaxVouchersPerChunk after 2+ retries:
        //   150 → 75 → 50 (floor).
        // Two-path logic based on whether the window can still shrink:
        //
        //   window > MinWindow  → throw ChunkOverloadedException (time-split, no Pass 2)
        //   window == MinWindow → enter 3-pass fallback (time cannot shrink further):
        //       Pass 2a: AccziteVoucherLedgers   → MergeLedgerEntries  (accounting data)
        //       Pass 2b: AccziteVoucherInventory → MergeInventoryEntries (stock data)
        //     Each pass is ~half the payload of AccziteVoucherDetail, preventing OOM.
        private int EffectiveCap => _scheduler.MaxVouchersPerChunk;

        // ── Two-pass voucher export ──────────────────────────────────────────────
        //
        // Old design: one request with AccziteVoucherPipeline fetching ALL fields
        //   (ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*, INVENTORYALLOCATIONS.*,
        //   TAXCLASSIFICATIONDETAILS.*) — Tally had to hold the entire response
        //   in memory before sending a single byte, which caused OOM crashes.
        //
        // New design:
        //   Pass 1 — AccziteVoucherHeaders (scalar fields only, ~1/10th the bytes)
        //     Collect voucher shells into an in-memory dictionary keyed by MASTERID.
        //     → Throw ChunkOverloadedException if count > MaxVouchersPerChunk and
        //       window > MinWindow so the controller can retry with a smaller window.
        //   Pass 2 — AccziteVoucherDetail (ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*)
        //     Stream ledger/inventory detail, merge into the header dictionary.
        //   Yield — iterate the completed dictionary; skip vouchers with no detail.

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
                        headers[voucher.TallyMasterId] = voucher;
                    else
                        metrics.RejectedCount++;
                }
            }

            // Empty chunk — nothing to do; let the scheduler grow the window.
            if (headers.Count == 0)
            {
                sw.Stop();
                metrics.Elapsed = sw.Elapsed;
                _scheduler.Adjust(metrics.Elapsed, 0, headerBytes, failed: false, isSafeMode: _syncMonitor.SyncMode == "Safe");
                yield break;
            }

            // ── Overload check ─────────────────────────────────────────────────────
            if (headers.Count > EffectiveCap)
            {
                if (_scheduler.CurrentWindow > _scheduler.MinWindow)
                {
                    // Time window can still shrink. Abort before the heavy detail pass.
                    // Controller will retry the same date range with a smaller window.
                    sw.Stop();
                    metrics.Elapsed      = sw.Elapsed;
                    metrics.PayloadBytes = headerBytes;
                    _scheduler.Adjust(metrics.Elapsed, headers.Count, headerBytes, failed: true, isSafeMode: _syncMonitor.SyncMode == "Safe");
                    throw new ChunkOverloadedException(headers.Count, EffectiveCap, _scheduler.CurrentWindow);
                }
                // Window is already at minimum. Fall through to 3-pass mode below.
            }

            // ── Pass 2 (or 3-pass fallback at MinWindow) ───────────────────────────
            // Normal path (count ≤ cap, or window > MinWindow didn't fire above):
            //   Single detail request: AccziteVoucherDetail (ALLLEDGERENTRIES.* + ALLINVENTORYENTRIES.*)
            //
            // 3-pass path (count > cap AND window == MinWindow):
            //   Pass 2a: AccziteVoucherLedgers   → accounting data + integrity check
            //   Pass 2b: AccziteVoucherInventory → root-level stock entries
            //   Each pass carries ~half the payload of AccziteVoucherDetail.
            var useThreePass = headers.Count > EffectiveCap &&
                               _scheduler.CurrentWindow <= _scheduler.MinWindow;

            var failed = true;
            try
            {
                if (!useThreePass)
                {
                    // ── Standard 2-pass detail WITH ID-Batching ─────────────────────
                    var maxIds = 25; // Safe TDL OR-chain emulation upper bound
                    var batchLists = headers.Values
                        .Select((v, i) => new { v, i })
                        .GroupBy(x => x.i / maxIds)
                        .Select(g => g.Select(x => x.v).ToList())
                        .ToList();

                    foreach (var batch in batchLists)
                    {
                        ct.ThrowIfCancellationRequested();

                        while (_syncMonitor.IsPaused)
                        {
                            _syncMonitor.TallyHealth = "Paused";
                            await Task.Delay(1000, ct);
                        }

                        var idList = string.Join(",", batch.Select(v => v.TallyMasterId));

                        try
                        {
                            await using var detailLease = await _tallyService.OpenCollectionXmlStreamAsync(
                                "AccziteVoucherDetail", range.Start, range.End, true, ct, idList: idList);

                            if (detailLease == null)
                                throw new InvalidOperationException($"Tally did not return a detail stream for ID batch.");

                            metrics.PayloadBytes += detailLease.DeclaredSize;
                            await MergeStreamAsync(detailLease.Stream, headers, metrics, ct,
                                (el, v) => { if (!_xmlParser.MergeVoucherDetail(el, v)) { metrics.RejectedCount++; return false; } return true; },
                                readerSettings);
                        }
                        catch (Exception ex)
                        {
                            _syncMonitor?.AddLog($"ID-Batch stream failed ({idList.Length} chars): {ex.Message}. Using fallback logic.", "WARNING", "FALLBACK");
                            
                            // 10-item fallback
                            var smallerBatches = batch
                                .Select((v, i) => new { v, i })
                                .GroupBy(x => x.i / 10)
                                .Select(g => g.Select(x => x.v).ToList());

                            foreach (var smBatch in smallerBatches)
                            {
                                var smIdList = string.Join(",", smBatch.Select(v => v.TallyMasterId));
                                await using var smLease = await _tallyService.OpenCollectionXmlStreamAsync(
                                    "AccziteVoucherDetail", range.Start, range.End, true, ct, idList: smIdList);
                                if (smLease != null)
                                {
                                    await MergeStreamAsync(smLease.Stream, headers, metrics, ct,
                                        (el, v) => { if (!_xmlParser.MergeVoucherDetail(el, v)) { metrics.RejectedCount++; return false; } return true; },
                                        readerSettings);
                                }
                            }
                        }

                        if (_syncMonitor.SyncMode == "Safe")
                        {
                            await Task.Delay(3000, ct);
                        }
                    }
                }
                else
                {
                    // ── 3-pass fallback: ledgers then root inventory ──────────────────
                    // Pass 2a: ALLLEDGERENTRIES.* — accounting + integrity check
                    await using var ledgerLease = await _tallyService.OpenCollectionXmlStreamAsync(
                        "AccziteVoucherLedgers", range.Start, range.End, true, ct);

                    if (ledgerLease == null)
                    {
                        metrics.Elapsed = sw.Elapsed;
                        _scheduler.Adjust(metrics.Elapsed, 0, headerBytes, failed: true, isSafeMode: _syncMonitor.SyncMode == "Safe");
                        throw new InvalidOperationException($"Tally did not return a ledger stream for {range}.");
                    }

                    metrics.PayloadBytes = headerBytes + ledgerLease.DeclaredSize;
                    await MergeStreamAsync(ledgerLease.Stream, headers, metrics, ct,
                        (el, v) => { if (!_xmlParser.MergeLedgerEntries(el, v)) { metrics.RejectedCount++; return false; } return true; },
                        readerSettings);

                    // Pass 2b: ALLINVENTORYENTRIES.* — root-level stock (much lighter)
                    await using var invLease = await _tallyService.OpenCollectionXmlStreamAsync(
                        "AccziteVoucherInventory", range.Start, range.End, true, ct);

                    if (invLease != null) // inventory pass is best-effort — not every company uses it
                    {
                        metrics.PayloadBytes += invLease.DeclaredSize;
                        // Use a null-metrics stand-in so FetchedCount is not double-counted.
                        var noCountMetrics = new VoucherChunkExecutionMetrics();
                        await MergeStreamAsync(invLease.Stream, headers, noCountMetrics, ct,
                            (el, v) => { _xmlParser.MergeInventoryEntries(el, v); return true; },
                            readerSettings);
                    }
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
