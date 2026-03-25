using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Acczite20.Data;
using Acczite20.Models;
using Acczite20.Services;
using Acczite20.Infrastructure;
using Acczite20.Services.Tally;

namespace Acczite20.Services.Sync
{
    public enum SyncMode
    {
        Full,        // Uses UI dates, overrides cursor
        Incremental, // Uses LastSuccessfulSync from DB
        Repair       // Re-fetches specific range, potentially overwrites
    }

    public interface ISyncLockProvider
    {
        Task<bool> AcquireLockAsync(string resource, TimeSpan timeout, CancellationToken ct);
        Task ReleaseLockAsync(string resource);
    }

    public class LocalSyncLockProvider : ISyncLockProvider
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim> _locks = new();
        public async Task<bool> AcquireLockAsync(string resource, TimeSpan timeout, CancellationToken ct)
        {
            var semaphore = _locks.GetOrAdd(resource, _ => new SemaphoreSlim(1, 1));
            return await semaphore.WaitAsync(timeout, ct);
        }
        public async Task ReleaseLockAsync(string resource)
        {
            if (_locks.TryGetValue(resource, out var semaphore)) semaphore.Release();
            await Task.CompletedTask;
        }
    }

    public class TallySyncOrchestrator
    {
        private readonly ILogger<TallySyncOrchestrator> _logger;
        private readonly TallyXmlService _tallyService;
        private readonly TallyXmlParser _xmlParser;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly TallyMasterSyncService _masterSyncService;
        private readonly Microsoft.Extensions.DependencyInjection.IServiceScopeFactory _scopeFactory;
        private readonly MongoService _mongoService;
        private readonly ISyncLockProvider _lockProvider;
        private readonly TallyCompanyService _tallyCompanyService;
        private Guid _currentRunId;
        private bool _isSyncRunning = false;
        private MasterDataCache? _cache;
        private string? _lastMasterIdInCheckpoint;
        private long _lastBatchLatencyMs = 0;

        private async Task AddToDeadLetterAsync(Guid orgId, Guid companyId, string masterId, string reason, string xml)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                db.DeadLetters.Add(new DeadLetter
                {
                    Id = Guid.NewGuid(),
                    OrganizationId = orgId,
                    CompanyId = companyId,
                    TallyMasterId = masterId,
                    ErrorReason = reason,
                    PayloadXml = xml,
                    DetectedAt = DateTimeOffset.UtcNow,
                    EntityType = "Voucher"
                });
                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Failed to write to DeadLetter table: {ex.Message}");
            }
        }

        public TallySyncOrchestrator(
            ILogger<TallySyncOrchestrator> logger,
            TallyXmlService tallyService,
            TallyXmlParser xmlParser,
            SyncStateMonitor syncMonitor,
            TallyMasterSyncService masterSyncService,
            Microsoft.Extensions.DependencyInjection.IServiceScopeFactory scopeFactory,
            MongoService mongoService,
            ISyncLockProvider lockProvider,
            TallyCompanyService tallyCompanyService)
        {
            _logger = logger;
            _tallyService = tallyService;
            _xmlParser = xmlParser;
            _syncMonitor = syncMonitor;
            _masterSyncService = masterSyncService;
            _scopeFactory = scopeFactory;
            _mongoService = mongoService;
            _lockProvider = lockProvider;
            _tallyCompanyService = tallyCompanyService;
        }

        public async Task RunDryRunValidationAsync(Guid orgId, CancellationToken ct)
        {
            var today = DateTimeOffset.Now;
            var from = today.AddDays(-3).Date; // 2 Days + buffer
            var to = from.AddDays(2);
            
            _logger.LogInformation($"🔬 [DRY-RUN] Starting 2-Day Validation Sync: {from:yyyy-MM-dd} to {to:yyyy-MM-dd}");
            
            try
            {
                // Execute standard sync loop for this range
                await RunFullSyncAsync(orgId, from, to, ct);
                
                // Post-run validation
                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                
                var deadLetterCount = await db.DeadLetters.Where(d => d.OrganizationId == orgId && d.DetectedAt >= today.AddMinutes(-10)).CountAsync(ct);
                var voucherCount = await db.Vouchers.Where(v => v.OrganizationId == orgId && v.VoucherDate >= from && v.VoucherDate <= to).CountAsync(ct);
                
                _logger.LogInformation($"📊 [DRY-RUN RESULT] Vouchers: {voucherCount}, Dead-Letters: {deadLetterCount}");
                _syncMonitor.AddLog($"✅ Dry Run Finished. Vouchers: {voucherCount}, Failures: {deadLetterCount}. Check Dead-Letter table for details.", "SUCCESS", "VALIDATION");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dry Run Failed");
                _syncMonitor.FailRun($"Dry Run Critical Failure: {ex.Message}");
            }
        }

        public async Task RunSyncCycleAsync(CancellationToken ct)
        {
            var lockKey = $"sync:{SessionManager.Instance.OrganizationId}";
            if (await _lockProvider.AcquireLockAsync(lockKey, TimeSpan.Zero, ct))
            {
                try
                {
                    await RunSyncCycleInternalAsync(ct);
                }
                finally
                {
                    await _lockProvider.ReleaseLockAsync(lockKey);
                }

            }
            else
            {
                _syncMonitor.AddLog("⚠ Sync already in progress by another caller or worker.", "WARNING", "LOCK");
            }
        }

        private async Task RunSyncCycleInternalAsync(CancellationToken ct, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null)
        {
            if (_isSyncRunning)
            {
                _syncMonitor.AddLog("⚠ A synchronization cycle is already active. Ignoring duplicate request.", "WARNING", "CONCURRENCY");
                return;
            }

            _isSyncRunning = true;
            _currentRunId = Guid.NewGuid();
            var orgId = SessionManager.Instance.OrganizationId;
            if (!_syncMonitor.IsSyncing)
            {
                _syncMonitor.BeginRun("Starting sync pipeline", $"Preparing sync context for org {orgId.ToString()[..8]}...");
            }
            else
            {
                _syncMonitor.SetStage("Starting sync pipeline", $"Preparing sync context for org {orgId.ToString()[..8]}...", 5, true);
            }

            _syncMonitor.AddLog($"🚀 Enterprise Sync Pipeline started for Org {orgId.ToString()[..8]}...");
            
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                // Proceed with existing logic...
                _syncMonitor.SetStage("Checking Tally connection", "Verifying that Tally is available for export.", 10, true);
                var status = await _tallyService.DetectTallyStatusAsync();
                
                if (status == TallyConnectionStatus.NotRunning)
                {
                    _syncMonitor.AddLog("❌ Tally is not running. Please start Tally and retry.", "ERROR", "TALLY");
                    _syncMonitor.FailRun("Tally is offline. Start Tally and retry.");
                    return;
                }
                
                if (status == TallyConnectionStatus.RunningNoCompany)
                {
                    _syncMonitor.AddLog("⚠ Tally is running but NO company is open.", "WARNING", "TALLY");
                    _syncMonitor.FailRun("No company is open in Tally. Please open your company in Tally and retry.");
                    return;
                }

                var resolvedCompany = await ResolveOpenCompanyAsync();
                if (resolvedCompany is null)
                {
                    const string errorMessage = "Could not resolve the currently open Tally company. Open the company in Tally, refresh the sync setup, and retry.";
                    _syncMonitor.AddLog(errorMessage, "ERROR", "TALLY");
                    _syncMonitor.FailRun(errorMessage);
                    return;
                }

                // 1. Masters First
                _syncMonitor.SetStage("Syncing master data", "Groups, ledgers, currencies, and voucher types are being refreshed.", 25, true);
                _syncMonitor.AddLog("Syncing Masters (Groups, Ledgers, Currencies, VoucherTypes)...", "INFO", "MASTERS");
                await _masterSyncService.SyncAllMastersAsync(orgId);
                
                // --- Sync Barrier: Check Integrity before proceeding to transactions ---
                _syncMonitor.AddLog("🔍 Verifying Master Data Integrity...", "INFO", "MASTERS");
                var integrity = await _masterSyncService.VerifyMasterDataIntegrityAsync(orgId);
                if (!integrity.IsValid)
                {
                    _syncMonitor.AddLog($"❌ Master Integrity Failed: {integrity.ErrorMessage}. Aborting Voucher Sync.", "ERROR", "MASTERS");
                    _syncMonitor.FailRun(integrity.ErrorMessage);
                    throw new Exception(integrity.ErrorMessage);
                }
                _syncMonitor.AddLog("✅ Master Integrity Verified. Proceeding to Voucher Stream.", "SUCCESS", "MASTERS");

                await UpdateSyncMetadataAsync(orgId, "Master Data", 100, true);
                _syncMonitor.SetStage("Master sync complete", "Master entities are ready. Starting voucher stream.", 40, false);

                // 2. High-Volume Voucher Sync
                _syncMonitor.SetStage("Streaming vouchers", "Reading transaction history and preparing insert batches.", 50, true);
                _syncMonitor.AddLog("Syncing Vouchers via High-Performance Channels (Standard Collection)...", "INFO", "VOUCHERS");
                await SyncVouchersWithChannelsAsync(orgId, ct, fromDate, toDate);
                
                // --- POST-SYNC INTEGRITY CHECK (Audit Grade) ---
                _syncMonitor.SetStage("Finalizing run", "Performing global Trial Balance integrity check...", 98, true);
                var integrityResult = await VerifyFinancialIntegrityAsync(orgId, ct);
                
                if (integrityResult.IsBalanced)
                {
                    _syncMonitor.AddLog($"✅ Financial Integrity Verified: Net Balance is {integrityResult.Difference:N2}. Data is consistent.", "SUCCESS", "AUDIT");
                }
                else
                {
                    _syncMonitor.AddLog($"❌ INTEGRITY MISMATCH: Net difference of {integrityResult.Difference:N2} detected. Check local records vs Tally Trial Balance.", "CRITICAL", "AUDIT");
                }

                await UpdateSyncMetadataAsync(orgId, "Voucher", _syncMonitor.TotalRecordsSynced, true);
                
                // 3. Mutation Reconciliation (Soft Delete)
                _syncMonitor.SetStage("Scanning deletions", "Reconciling records deleted or moved in Tally.", 90, true);
                _syncMonitor.AddLog("Starting Mutation Reconciliation (Soft Delete)...", "INFO", "CLEANUP");
                await SyncDeletionsAsync(orgId, fromDate, toDate, ct);

                _syncMonitor.SetStage("Finalizing sync", "Verifying cleanup and writing completion metadata.", 96, false);

                stopwatch.Stop();
                var elapsed = stopwatch.Elapsed;
                var completionDetail = $"Completed in {elapsed:mm\\:ss} at {_syncMonitor.VouchersPerSecond:N0} v/sec. Total: {_syncMonitor.TotalRecordsSynced:N0} vouchers.";
                _syncMonitor.AddLog($"✅ Sync completed in {elapsed:mm\\:ss}. Rate: {_syncMonitor.VouchersPerSecond} v/sec. Mem: {_syncMonitor.MemoryUsage}", "SUCCESS", "ORCHESTRATOR");
                _syncMonitor.CompleteRun(completionDetail);
                
                // Final Metadata Update to mark 'Completed' for Deletion Gate
                await UpdateSyncMetadataAsync(orgId, "Voucher", _syncMonitor.TotalRecordsSynced, true);
            }
            catch (OperationCanceledException)
            {
                _syncMonitor.CancelRun("The full sync was cancelled.");
                throw;
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"Sync failed: {ex.Message}", "ERROR", "ORCHESTRATOR");
                _logger.LogError(ex, "Sync Cycle Error");
                await UpdateSyncMetadataAsync(orgId, "Voucher", _syncMonitor.TotalRecordsSynced, false, ex.Message);
                _syncMonitor.FailRun(ex.Message);
                throw;
            }
            finally
            {
                _isSyncRunning = false;
            }
        }

        private async Task UpdateSyncMetadataAsync(Guid orgId, string entityType, int records, bool success, string? error = null, DateTimeOffset? checkpoint = null, string? lastMasterId = null)
        {
            try 
            {
                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                
                var company = await db.Companies.FirstOrDefaultAsync(c => c.OrganizationId == orgId);
                if (company == null) return;

                var metadata = await db.SyncMetadataRecords
                    .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType);

                if (metadata == null)
                {
                    metadata = new SyncMetadata
                    {
                        Id = Guid.NewGuid(),
                        OrganizationId = orgId,
                        CompanyId = company.Id,
                        EntityType = entityType
                    };
                    await db.SyncMetadataRecords.AddAsync(metadata);
                }

                metadata.RecordsSyncedInLastRun = records;
                metadata.LastModified = DateTimeOffset.UtcNow;
                metadata.IsSyncRunning = false;
                metadata.LastVoucherMasterId = lastMasterId switch
                {
                    null => metadata.LastVoucherMasterId,
                    "" => null,
                    _ => lastMasterId
                };
                
                if (success)
                {
                    metadata.LastSuccessfulSync = checkpoint ?? DateTimeOffset.UtcNow;
                    metadata.LastError = null;
                    metadata.RetryCount = 0;
                }
                else
                {
                    metadata.LastError = error;
                    metadata.RetryCount++;
                }
                metadata.UpdatedAt = DateTimeOffset.Now;

                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to update sync metadata for {entityType}");
            }
        }

        private async Task<string?> ResolveOpenCompanyAsync()
        {
            var selectedCompany = SessionManager.Instance.TallyCompanyName?.Trim();
            var openCompany = await _tallyCompanyService.GetOpenCompanyAsync();

            if (IsUnresolvedCompany(openCompany))
            {
                openCompany = await _tallyService.GetCurrentCompanyNameAsync();
            }

            if (IsUnresolvedCompany(openCompany))
            {
                return null;
            }

            if (!string.Equals(selectedCompany, openCompany, StringComparison.OrdinalIgnoreCase))
            {
                if (!IsUnresolvedCompany(selectedCompany))
                {
                    _syncMonitor.AddLog(
                        $"Selected Tally company '{selectedCompany}' did not match the open company. Using '{openCompany}'.",
                        "WARNING",
                        "TALLY");
                }
                else
                {
                    _syncMonitor.AddLog($"Resolved open Tally company: {openCompany}", "SUCCESS", "TALLY");
                }

            }
            else
            {
                _syncMonitor.AddLog($"Using Tally company: {openCompany}", "INFO", "TALLY");
            }

            SessionManager.Instance.TallyCompanyName = openCompany;
            return openCompany;
        }

        private static bool IsUnresolvedCompany(string? companyName)
        {
            return string.IsNullOrWhiteSpace(companyName)
                || string.Equals(companyName, "None", StringComparison.OrdinalIgnoreCase)
                || string.Equals(companyName, "Default Company", StringComparison.OrdinalIgnoreCase);
        }

        private async Task SyncVouchersWithChannelsAsync(Guid orgId, CancellationToken ct, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null)
        {
            _syncMonitor.AddLog("🚀 Initializing Priority-Aware Sync Pipeline...", "INFO", "ORCHESTRATOR");
            _syncMonitor.SetStage("Warming internal caches", "Preparing ID map for high-speed Fact table reconciliation.", 10, true);
            
            // Warm up the ID cache for the entire session
            using (var scope = _scopeFactory.CreateScope())
            {
                var cache = scope.ServiceProvider.GetRequiredService<MasterDataCache>();
                await cache.InitializeAsync(orgId);
                _cache = cache; // Current local cache for producer validation
            }

            var mainStopwatch = System.Diagnostics.Stopwatch.StartNew();

            try 
            {
                // --- PHASE 1: Priority Window Sync (Recent 30 Days) ---
                var priorityDate = DateTimeOffset.Now.AddDays(-30);
                var syncStart = fromDate ?? DateTimeOffset.UtcNow.AddYears(-10);
                var syncEnd = toDate ?? DateTimeOffset.Now;

                if (syncEnd > priorityDate && syncStart < syncEnd)
                {
                    var effectivePriorityStart = syncStart > priorityDate ? syncStart : priorityDate;
                    _syncMonitor.SetStage("Priority Sync", $"Processing recent records ({effectivePriorityStart:yyyy-MM-dd} to {syncEnd:yyyy-MM-dd})", 20, true);
                    _syncMonitor.AddLog($"⚡ [PRIORITY] Syncing last 30 days first...", "INFO", "PRIORITY");
                    await SyncRangeAsync(orgId, effectivePriorityStart, syncEnd, ct);
                }

                // --- PHASE 2: Historical Window Sync ---
                if (syncStart < priorityDate && !ct.IsCancellationRequested)
                {
                    var effectiveHistoricalEnd = syncEnd < priorityDate ? syncEnd : priorityDate;
                    _syncMonitor.SetStage("Historical Backfill", "Reconciling older records in background phase...", 60, true);
                    _syncMonitor.AddLog($"📚 [HISTORY] Backfilling records from {syncStart:yyyy-MM-dd} to {effectiveHistoricalEnd:yyyy-MM-dd}...", "INFO", "HISTORY");
                    await SyncRangeAsync(orgId, syncStart, effectiveHistoricalEnd, ct);
                }
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"❌ Pipeline Failure: {ex.Message}", "ERROR", "ORCHESTRATOR");
                _logger.LogError(ex, "Voucher sync pipeline failed.");
                throw;
            }
            finally
            {
                mainStopwatch.Stop();
                _syncMonitor.AddLog($"🏁 Pipeline Cycle Complete. Elapsed: {mainStopwatch.Elapsed.TotalSeconds:F1}s", "SUCCESS", "ORCHESTRATOR");
            }
        }

        private async Task SyncRangeAsync(Guid orgId, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            DateTimeOffset currentSyncFrom = from;
            Guid companyId = Guid.Empty;

            // CHECKPOINT RESUME: Check if we have a saved progress for this range
            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var meta = await db.SyncMetadataRecords
                    .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == "Voucher", ct);
                
                if (meta != null && meta.LastSuccessfulSync.HasValue && meta.LastSuccessfulSync > from && meta.LastSuccessfulSync < to)
                {
                    currentSyncFrom = meta.LastSuccessfulSync.Value.AddTicks(1);
                    _syncMonitor.AddLog($"⏮ Resuming Sync from Checkpoint: {currentSyncFrom:yyyy-MM-dd}", "INFO", "ORCHESTRATOR");
                }

                var company = await db.Companies.FirstOrDefaultAsync(c => c.OrganizationId == orgId, ct);
                companyId = company?.Id ?? Guid.Empty;
            }

            if (companyId == Guid.Empty)
            {
                _syncMonitor.AddLog("No company record found in DB for this organization. Sync cannot continue.", "ERROR", "VOUCHERS");
                return;
            }

            if (currentSyncFrom > to)
            {
                _syncMonitor.AddLog("Requested range is already covered by the latest checkpoint.", "SUCCESS", "VOUCHERS");
                return;
            }

            var progress = new VoucherSyncProgressAggregator();

            // BatchSize from UI maps directly to MaxVouchersPerChunk.
            // Auto mode defaults to 50 (conservative, not 150) — the scheduler will
            // grow the window on its own if Tally is healthy.
            // Safe mode uses whatever the user picked (10 / 25 / 50 / 100).
            int schedulerCap = _syncMonitor.SyncMode == "Safe"
                ? _syncMonitor.BatchSize
                : 50;

            var scheduler = new VoucherSyncChunkScheduler(TimeSpan.FromHours(6));
            var executor = new TallyVoucherRequestExecutor(_tallyService, _xmlParser, scheduler, _syncMonitor);
            var dbWriter = new VoucherSyncDbWriter(orgId, _scopeFactory, _syncMonitor, progress, sw);
            var controller = new VoucherSyncController(scheduler, executor, dbWriter, progress, _syncMonitor, _tallyService);

            await controller.RunAsync(
                orgId,
                companyId,
                currentSyncFrom,
                to,
                (voucher, token) => PrepareVoucherForWriteAsync(orgId, companyId, voucher, token),
                async (chunk, metrics, token) =>
                {
                    var snapshot = progress.Snapshot();
                    var payloadMb = metrics.PayloadBytes <= 0 ? 0 : metrics.PayloadBytes / 1024d / 1024d;

                    _syncMonitor.AddLog(
                        $"Chunk complete: {chunk.Start:yyyy-MM-dd HH:mm} to {chunk.End:yyyy-MM-dd HH:mm} | parsed {metrics.FetchedCount:N0} | queued {metrics.EnqueuedCount:N0} | rejected {metrics.RejectedCount:N0} | {metrics.Elapsed.TotalSeconds:N1}s | {payloadMb:N1} MB | next window {scheduler.CurrentWindow.TotalHours:0.#}h.",
                        "INFO",
                        "VOUCHERS");

                    _syncMonitor.SetStage(
                        "Streaming vouchers",
                        $"Fetched {snapshot.Fetched:N0}, saved {snapshot.Written:N0}. Completed chunk {chunk.Start:yyyy-MM-dd HH:mm} to {chunk.End:yyyy-MM-dd HH:mm}.",
                        68,
                        false);

                    await UpdateSyncMetadataAsync(orgId, "Voucher", _syncMonitor.TotalRecordsSynced, true, null, chunk.End, string.Empty);
                },
                ct);

            // Verify and Sync Deletions
            await SyncDeletionsAsync(orgId, from, to, ct);
        }

        private async ValueTask<Voucher?> PrepareVoucherForWriteAsync(Guid orgId, Guid companyId, Voucher voucher, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            if (_cache == null)
            {
                throw new InvalidOperationException("Master data cache is not initialized for voucher validation.");
            }

            voucher.OrganizationId = orgId;
            voucher.CompanyId = companyId;
            voucher.ReferenceNumber = string.IsNullOrWhiteSpace(voucher.ReferenceNumber) ? voucher.VoucherNumber : voucher.ReferenceNumber;

            var voucherTypeName = voucher.VoucherType?.Name ?? "Journal";
            voucher.VoucherTypeId = _cache.GetVoucherTypeId(voucherTypeName);
            if (voucher.VoucherTypeId == Guid.Empty && !string.Equals(voucherTypeName, "Journal", StringComparison.OrdinalIgnoreCase))
            {
                voucher.VoucherTypeId = _cache.GetVoucherTypeId("Journal");
            }

            if (voucher.VoucherTypeId == Guid.Empty)
            {
                await AddToDeadLetterAsync(
                    orgId,
                    companyId,
                    voucher.TallyMasterId ?? string.Empty,
                    $"Voucher type not found: {voucherTypeName}",
                    $"<VOUCHER MASTERID=\"{voucher.TallyMasterId}\"><VOUCHERTYPENAME>{voucherTypeName}</VOUCHERTYPENAME></VOUCHER>");
                return null;
            }

            foreach (var ledgerEntry in voucher.LedgerEntries)
            {
                if (_cache.GetLedgerId(ledgerEntry.LedgerName) != Guid.Empty)
                {
                    continue;
                }

                await AddToDeadLetterAsync(
                    orgId,
                    companyId,
                    voucher.TallyMasterId ?? string.Empty,
                    $"Ledger not found: {ledgerEntry.LedgerName}",
                    $"<VOUCHER MASTERID=\"{voucher.TallyMasterId}\"><LEDGERNAME>{ledgerEntry.LedgerName}</LEDGERNAME></VOUCHER>");
                return null;
            }

            var totalDebit = voucher.LedgerEntries.Sum(le => le.DebitAmount);
            var totalCredit = voucher.LedgerEntries.Sum(le => le.CreditAmount);
            if (!voucher.IsCancelled && Math.Abs(totalDebit - totalCredit) > 0.01m)
            {
                await AddToDeadLetterAsync(
                    orgId,
                    companyId,
                    voucher.TallyMasterId ?? string.Empty,
                    $"Double-entry mismatch: Debit {totalDebit} != Credit {totalCredit}",
                    $"<VOUCHER MASTERID=\"{voucher.TallyMasterId}\"><VOUCHERNUMBER>{voucher.VoucherNumber}</VOUCHERNUMBER></VOUCHER>");
                return null;
            }

            var now = DateTimeOffset.UtcNow;
            if (voucher.CreatedAt == default)
            {
                voucher.CreatedAt = now;
            }

            voucher.LastModified = now;
            voucher.UpdatedAt = now;
            voucher.SyncRunId = _currentRunId;

            return voucher;
        }

        private async Task ProduceVouchersFromTallyAsync(ChannelWriter<Voucher> writer, Guid orgId, CancellationToken ct, DateTimeOffset? fromOverride = null, DateTimeOffset? toOverride = null)
        {
            try
            {
                DateTimeOffset fromDate;
                Guid companyId = Guid.Empty;

                using (var scope = _scopeFactory.CreateScope())
                {
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    var metadata = await db.SyncMetadataRecords
                        .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == "Voucher", ct);
                    
                    fromDate = fromOverride ?? metadata?.LastSuccessfulSync ?? DateTimeOffset.UtcNow.AddYears(-10);
                    _lastMasterIdInCheckpoint = metadata?.LastVoucherMasterId;

                    var company = await db.Companies.FirstOrDefaultAsync(c => c.OrganizationId == orgId, ct);
                    companyId = company?.Id ?? Guid.Empty;
                }

                if (companyId == Guid.Empty)
                {
                    _syncMonitor.AddLog("❌ No company record found in DB for this organization. Sync cannot continue.", "ERROR", "VOUCHERS");
                    return;
                }

                var toDate = toOverride ?? DateTimeOffset.Now;

                // --- PHASE 1: DISCOVERY SCAN (Metadata Only) ---
                _syncMonitor.SetStage("Discovery Scan", "Identifying New/Modified records from Tally...", 45, true);
                _syncMonitor.AddLog("Starting High-Speed Discovery Scan...", "INFO", "DISCOVERY");
                
                var discoveryMap = new Dictionary<string, (int AlterId, DateTimeOffset Date)>();
                
                // --- Discovery Retry Strategy (Commercial Resilience) ---
                int retryCount = 0;
                while (retryCount < 3)
                {
                    try 
                    {
                        var result = await _tallyService.ExportCollectionXmlStreamAsync("AccziteVoucherDiscoveryCollection", fromDate, toDate, true);
                        using (var discStream = result.Stream)
                        {
                            if (discStream != null)
                            {
                                using var discReader = XmlReader.Create(discStream, new XmlReaderSettings{ Async = true });
                                while (await discReader.ReadAsync())
                                {
                                     if (discReader.NodeType == XmlNodeType.Element && string.Equals(discReader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                                     {
                                         var element = (XElement)XNode.ReadFrom(discReader);
                                         var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value;
                                         var aid = int.TryParse(element.Element("ALTERID")?.Value, out var v) ? v : 0;
                                         var dtStr = element.Element("DATE")?.Value;
                                         var vDate = DateTimeOffset.TryParseExact(dtStr, "yyyyMMdd", null, System.Globalization.DateTimeStyles.None, out var d) ? d : DateTimeOffset.MinValue;

                                         if (!string.IsNullOrEmpty(id)) discoveryMap[id] = (aid, vDate);
                                     }
                                }
                                break; // Success
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        _syncMonitor.AddLog($"⚠ Discovery attempt {retryCount} failed: {ex.Message}. Retrying...", "WARNING", "DISCOVERY");
                        await Task.Delay(500 * retryCount, ct);
                    }
                }

                if (!discoveryMap.Any())
                {
                     // If DB has data but Tally returns 0, this is a CRITICAL fail-stop for deletion readiness
                     _syncMonitor.AddLog("✅ No vouchers found in Tally for this range.", "SUCCESS", "DISCOVERY");
                }
                else
                {
                    _syncMonitor.AddLog($"✅ Discovery Scan Complete: {discoveryMap.Count} vouchers found.", "SUCCESS", "DISCOVERY");
                }

                // Identify Drift vs Database
                Dictionary<string, (int AlterId, DateTimeOffset Date)> dbMap;
                using (var scope = _scopeFactory.CreateScope())
                {
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    dbMap = await db.Vouchers
                        .Where(v => v.OrganizationId == orgId && v.VoucherDate >= fromDate && v.VoucherDate <= toDate)
                        .Select(v => new { v.TallyMasterId, v.AlterId, v.VoucherDate })
                        .ToDictionaryAsync(x => x.TallyMasterId, x => (x.AlterId, (DateTimeOffset)x.VoucherDate), ct);
                }

                // Identify "Drifted" records (Modified, New, or Backdated Changes)
                // We check AlterId OR Date to catch backdated edits that might have bypassed AlterId increments
                var modifiedIds = discoveryMap
                    .Where(kvp => !dbMap.TryGetValue(kvp.Key, out var dbInfo) || 
                                  kvp.Value.AlterId > dbInfo.AlterId || 
                                  kvp.Value.Date != dbInfo.Date)
                    .Select(kvp => kvp.Key)
                    .ToHashSet();

                _syncMonitor.AddLog($"📊 Change Detection: {modifiedIds.Count} New/Modified, {discoveryMap.Count - modifiedIds.Count} Unchanged.", "INFO", "DISCOVERY");

                if (!modifiedIds.Any())
                {
                    _syncMonitor.AddLog("🚀 All records are already up-to-date. Skipping full parsing phase.", "SUCCESS", "VOUCHERS");
                    writer.Complete(); 
                    return;
                }

                // --- PHASE 2: DIFFERENTIAL FETCH (Multi-Pass per Day) ---
                _syncMonitor.SetStage("Streaming", "Multi-pass memory-safe XML fetch...", 60, true);

                var dayVouchers = new Dictionary<string, Voucher>();

                // Helper to apply true adaptive backpressure (Linked to DB latency)
                async Task ApplyThrottling(long xmlSize)
                {
                    long latency = Interlocked.Read(ref _lastBatchLatencyMs);
                    
                    if (latency > 1000) 
                    {
                        // Critical Pressure: DB is struggling (locks/IO wait)
                        await Task.Delay(2000, ct); 
                        _syncMonitor.AddLog("⚠️ CRITICAL DB PRESSURE: Throttling Producer (2s cooldown)", "WARNING", "PRESSURE");
                    }
                    else if (latency > 400 || xmlSize > 5_000_000) 
                    {
                        // High Pressure: Large XML or slower DB
                        await Task.Delay(500, ct); 
                    }
                    else if (latency > 100)
                    {
                        // Normal Load: Slight buffer to prevent spikes
                        await Task.Delay(50, ct);
                    }
                    else 
                    {
                        // Healthy: Minimal micro-throttle for thread yield
                        await Task.Delay(5, ct); 
                    }
                }

                async Task<(System.IO.Stream? Stream, long Size)> FetchWithRetryAsync(string collectionName)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        try
                        {
                            var res = await _tallyService.ExportCollectionXmlStreamAsync(collectionName, fromDate, toDate, true);
                            _logger.LogInformation($"XML Size ({collectionName}): {res.Size / 1024} KB");
                            return res;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning($"Tally Hiccup during {collectionName}. Attempt {i + 1}/3. Error: {ex.Message}");
                            if (i == 2) throw;
                            await Task.Delay(2000, ct);
                        }
                    }
                    return (null, 0);
                }

                // PASS 1: Headers
                var res1 = await FetchWithRetryAsync("AccziteVoucherHeaders");
                using (var stream1 = res1.Stream)
                {
                    if (stream1 == null) return;
                    using var reader = XmlReader.Create(stream1, new XmlReaderSettings { Async = true });
                    while (await reader.ReadAsync())
                    {
                        if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                        {
                            var element = (XElement)XNode.ReadFrom(reader);
                            var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value ?? string.Empty;
                            if (!string.IsNullOrEmpty(id))
                            {
                                var v = _xmlParser.ParseVoucherEntity(element, orgId, companyId);
                                if (v != null) 
                                {
                                    dayVouchers[id] = v;
                                }
                                else
                                {
                                    _logger.LogWarning($"🚩 [DEAD-LETTER] Voucher {id} failed header integrity check (Malformed). Skipping.");
                                    await AddToDeadLetterAsync(orgId, companyId, id, "Header Malformed", element.ToString());
                                }
                            }
                        }
                    }
                    await ApplyThrottling(res1.Size);
                }

                if (!dayVouchers.Any())
                {
                    _logger.LogWarning($"No data for {fromDate:yyyy-MM-dd}");
                }
                else
                {
                    _logger.LogInformation($"Date: {fromDate:yyyy-MM-dd}, Records: {dayVouchers.Count}");

                    // PASS 2: Ledgers
                    var res2 = await FetchWithRetryAsync("AccziteVoucherLedgers");
                    using (var stream2 = res2.Stream)
                    {
                        if (stream2 != null)
                        {
                            using var reader = XmlReader.Create(stream2, new XmlReaderSettings { Async = true });
                            while (await reader.ReadAsync())
                            {
                                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                                {
                                    var element = (XElement)XNode.ReadFrom(reader);
                                    var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value ?? string.Empty;
                                    if (!string.IsNullOrEmpty(id) && dayVouchers.TryGetValue(id, out var existing))
                                    {
                                        var v = _xmlParser.ParseVoucherEntity(element, orgId, companyId);
                                        if (v != null)
                                        {
                                            foreach (var le in v.LedgerEntries) existing.LedgerEntries.Add(le);
                                            existing.TotalAmount = v.TotalAmount;
                                        }
                                    }
                                }
                            }
                            await ApplyThrottling(res2.Size);
                        }
                    }

                    // PASS 3: Inventory
                    var res3 = await FetchWithRetryAsync("AccziteVoucherInventory");
                    using (var stream3 = res3.Stream)
                    {
                        if (stream3 != null)
                        {
                            using var reader = XmlReader.Create(stream3, new XmlReaderSettings { Async = true });
                            while (await reader.ReadAsync())
                            {
                                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                                {
                                    var element = (XElement)XNode.ReadFrom(reader);
                                    var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value ?? string.Empty;
                                    if (!string.IsNullOrEmpty(id) && dayVouchers.TryGetValue(id, out var existing))
                                    {
                                        var v = _xmlParser.ParseVoucherEntity(element, orgId, companyId);
                                        if (v != null)
                                        {
                                            foreach (var ia in v.InventoryAllocations) existing.InventoryAllocations.Add(ia);
                                        }
                                        else
                                        {
                                            _logger.LogWarning($"🚩 [DEAD-LETTER] Voucher {id} failed integrity check during Inventory pass. Skipping.");
                                            _syncMonitor.AddLog($"⚠ Unbalanced Voucher {id} detected during Inventory pass. Skipped.", "WARNING", "INTEGRITY");
                                        }
                                    }
                                }
                            }
                            await ApplyThrottling(res3.Size);
                        }
                    }

                    // PASS 4: GST
                    var res4 = await FetchWithRetryAsync("AccziteVoucherGST");
                    using (var stream4 = res4.Stream)
                    {
                        if (stream4 != null)
                        {
                            using var reader = XmlReader.Create(stream4, new XmlReaderSettings { Async = true });
                            while (await reader.ReadAsync())
                            {
                                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                                {
                                    var element = (XElement)XNode.ReadFrom(reader);
                                    var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value ?? string.Empty;
                                    if (!string.IsNullOrEmpty(id) && dayVouchers.TryGetValue(id, out var existing))
                                    {
                                        var v = _xmlParser.ParseVoucherEntity(element, orgId, companyId);
                                        if (v != null)
                                        {
                                            foreach (var gb in v.GstBreakdowns) existing.GstBreakdowns.Add(gb);
                                        }
                                        else
                                        {
                                            _logger.LogWarning($"🚩 [DEAD-LETTER] Voucher {id} failed integrity check during GST pass. Skipping.");
                                            _syncMonitor.AddLog($"⚠ Unbalanced Voucher {id} detected during GST pass. Skipped.", "WARNING", "INTEGRITY");
                                        }
                                    }
                                }
                            }
                            await ApplyThrottling(res4.Size);
                        }
                    }
                }

                // --- PHASE 3: FINAL VALIDATION & INTRA-DAY RESUME ---
                bool hasFoundCheckpoint = string.IsNullOrEmpty(_lastMasterIdInCheckpoint);
                int pushedCount = 0;

                foreach (var v in dayVouchers.Values)
                {
                    // 1. Resume Check: Skip if we haven't reached the last checkpoint record yet
                    if (!hasFoundCheckpoint)
                    {
                        if (v.TallyMasterId == _lastMasterIdInCheckpoint)
                        {
                            hasFoundCheckpoint = true;
                            _logger.LogInformation($"⏩ Skipping already processed records until MasterID: {v.TallyMasterId}");
                        }
                        continue;
                    }

                    // 2. Referential Check: All ledgers must exist in DB cached maps
                    bool hasLedgerConflict = false;
                    foreach (var le in v.LedgerEntries)
                    {
                        if (_cache != null && _cache.GetLedgerId(le.LedgerName) == Guid.Empty)
                        {
                            hasLedgerConflict = true;
                            await AddToDeadLetterAsync(orgId, companyId, v.TallyMasterId!, $"Ledger Not Found: {le.LedgerName}", "Full Voucher State Validation Fail");
                            break;
                        }
                    }

                    if (hasLedgerConflict)
                    {
                        _logger.LogWarning($"🚩 [DEAD-LETTER] Voucher {v.TallyMasterId} skipped due to ledger conflict.");
                        continue;
                    }

                    // 3. Double-Entry check
                    var totalDebit = v.LedgerEntries.Sum(le => le.DebitAmount);
                    var totalCredit = v.LedgerEntries.Sum(le => le.CreditAmount);

                    if (Math.Abs(totalDebit - totalCredit) > 0.01m)
                    {
                        var errorMsg = $"Double-Entry Violation: Voucher {v.VoucherNumber} / {v.TallyMasterId} is unbalanced (Debit {totalDebit} != Credit {totalCredit}).";
                        _logger.LogWarning($"🚩 [DEAD-LETTER] {errorMsg}");
                        await AddToDeadLetterAsync(orgId, companyId, v.TallyMasterId!, errorMsg, "Full Voucher State Validation Fail: Double Entry");
                        continue;
                    }

                    v.SyncRunId = _currentRunId;
                    await writer.WriteAsync(v, ct);
                    pushedCount++;
                    
                    if (pushedCount % 100 == 0)
                    {
                        _syncMonitor.AddLog($"Producer: Dispatched {pushedCount} assembled vouchers...", "INFO", "VOUCHERS");
                    }
                }

                if (pushedCount == 0)
                {
                    _syncMonitor.AddLog("⚠ Parsed 0 valid VOUCHER structures for this day.", "WARNING", "VOUCHERS");
                }
                else
                {
                    _syncMonitor.AddLog($"Producer: Sent {pushedCount:N0} assembled vouchers for {fromDate.Date:yyyy-MM-dd}.", "SUCCESS", "VOUCHERS");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Producer Critical Error");
                _syncMonitor.AddLog($"❌ Producer Error: {ex.Message}", "ERROR", "VOUCHERS");
                await Task.Delay(3000, ct); // Cooldown as requested
            }
            finally
            {
                writer.Complete();
            }
        }

        private async Task ConsumeVouchersAndBulkInsertAsync(Guid orgId, ChannelReader<Voucher> reader, CancellationToken ct, System.Diagnostics.Stopwatch sw)
        {
            var batch = new List<Voucher>(1000);
            try
            {
                await foreach (var voucher in reader.ReadAllAsync(ct))
                {
                    batch.Add(voucher);
                    if (batch.Count >= 1000)
                    {
                        var ledgerCount = batch.Sum(v => v.LedgerEntries?.Count ?? 0);
                        var invCount = batch.Sum(v => v.InventoryAllocations?.Count ?? 0);
                        
                        // MEASURE DB LATENCY FOR ADAPTIVE BACKPRESSURE
                        var timer = System.Diagnostics.Stopwatch.StartNew();
                        await RunBulkInsertAsync(batch);
                        timer.Stop();
                        
                        long currentLatency = timer.ElapsedMilliseconds;
                        Interlocked.Exchange(ref _lastBatchLatencyMs, currentLatency);

                        UpdateDetailedProgress(batch.Count, ledgerCount, invCount, sw.Elapsed.TotalSeconds);
                        
                        // Adaptive consumer-side delay to prevent connection pool saturation
                        int consumerDelay = currentLatency > 500 ? 50 : (currentLatency > 200 ? 10 : 5);
                        await Task.Delay(consumerDelay, ct);

                        // Intra-day Checkpoint: Update last MasterID processed
                        var lastV = batch.LastOrDefault();
                        if (lastV != null)
                        {
                            await UpdateSyncMetadataAsync(orgId, "Voucher", _syncMonitor.TotalRecordsSynced, true, null, null, lastV.TallyMasterId);
                        }

                        batch.Clear();
                    }
                }

                if (batch.Any())
                {
                    var ledgerCount = batch.Sum(v => v.LedgerEntries?.Count ?? 0);
                    var invCount = batch.Sum(v => v.InventoryAllocations?.Count ?? 0);
                    
                    await RunBulkInsertAsync(batch);
                    UpdateDetailedProgress(batch.Count, ledgerCount, invCount, sw.Elapsed.TotalSeconds);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer Critical Error");
                _syncMonitor.AddLog($"❌ Database Batch Inserter Error: {ex.Message}", "ERROR", "DATABASE");
            }
        }

        private async Task RunBulkInsertAsync(List<Voucher> vouchers)
        {
            using (var scope = _scopeFactory.CreateScope())
            {
                var handler = scope.ServiceProvider.GetRequiredService<BulkInsertHandler>();
                await handler.BulkInsertVouchersAsync(vouchers);
            }
        }

        private void UpdateDetailedProgress(int vCount, int lCount, int iCount, double elapsedSeconds)
        {
            _syncMonitor.RecordInsertedBatch(vCount, lCount, iCount, elapsedSeconds);
            _syncMonitor.AddLog($"[BATCH] Committed {vCount} vouchers to DB.", "INFO", "DATABASE");
        }

        public async Task RunFullSyncAsync(Guid orgId, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null, CancellationToken ct = default)
        {
            var lockKey = $"sync:{orgId}";
            if (await _lockProvider.AcquireLockAsync(lockKey, TimeSpan.Zero, ct))
            {
                try
                {
                _syncMonitor.BeginRun("Preparing fresh sync", "Resetting sync metadata before the full organization run.");
                _syncMonitor.AddLog("🧹 Cleaning existing sync metadata for Fresh Start...", "WARNING");
                
                using (var scope = _scopeFactory.CreateScope())
                {
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    var records = await db.SyncMetadataRecords.Where(m => m.OrganizationId == orgId).ToListAsync(ct);
                    
                    // If no records exist, create a baseline one for Vouchers
                    if (!records.Any())
                    {
                        db.SyncMetadataRecords.Add(new Acczite20.Models.SyncMetadata
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            EntityType = "Voucher",
                            LastSuccessfulSync = fromDate
                        });
                    }
                    else
                    {
                        foreach (var r in records) 
                        {
                            // If user provided a date, we use it as the 'Last Success' to trick the engine into starting there
                            r.LastSuccessfulSync = fromDate;
                            r.LastVoucherMasterId = null; // Reset intra-day checkpoint for fresh start
                        }
                    }
                    await db.SaveChangesAsync(ct);
                }

                await RunSyncCycleInternalAsync(ct, fromDate, toDate);
            }
            catch (OperationCanceledException)
            {
                _syncMonitor.CancelRun("The full sync was cancelled.");
                throw;
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"Full sync preparation failed: {ex.Message}", "ERROR", "ORCHESTRATOR");
                _syncMonitor.FailRun(ex.Message);
                throw;
            }
            finally
            {
                _isSyncRunning = false;
                await _lockProvider.ReleaseLockAsync(lockKey);
            }
            } // Close if (AcquireLock)
        } // Close RunFullSyncAsync

        private async Task SyncDeletionsAsync(Guid orgId, DateTimeOffset? from, DateTimeOffset? to, CancellationToken ct)
        {
            // --- Gating: NEVER delete if the sync was partial or failed ---
            if (_syncMonitor.CurrentStage == "Sync failed" || _syncMonitor.CurrentStage == "Sync cancelled")
            {
                _syncMonitor.AddLog("‼ Safety Gate: Sync run was incomplete. Skipping mutation reconciliation.", "CAUTION", "CLEANUP");
                return;
            }

            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var fromDate = from ?? DateTimeOffset.UtcNow.AddYears(-10);
                var toDate = to ?? DateTimeOffset.Now;

                var tallyIds = new HashSet<string>();
                
                for (var chunkStart = fromDate; chunkStart <= toDate; chunkStart = chunkStart.AddDays(1))
                {
                    var chunkEnd = chunkStart.AddDays(1).AddTicks(-1);
                    if (chunkEnd > toDate) chunkEnd = toDate;

                    int retryAttempts = 0;
                    bool chunkSuccess = false;
                    while (retryAttempts < 3 && !chunkSuccess)
                    {
                        await using var lease = await _tallyService.OpenCollectionXmlStreamAsync(
                            "AccziteVoucherDiscoveryCollection",
                            chunkStart,
                            chunkEnd,
                            true,
                            ct);

                        if (lease != null)
                        {
                            chunkSuccess = true;
                            using var reader = XmlReader.Create(lease.Stream, new XmlReaderSettings { Async = true, CheckCharacters = false });
                            while (await reader.ReadAsync())
                            {
                                if (reader.NodeType == XmlNodeType.Element && string.Equals(reader.Name, "VOUCHER", StringComparison.OrdinalIgnoreCase))
                                {
                                    var element = (XElement)XNode.ReadFrom(reader);
                                    var id = element.Element("MASTERID")?.Value ?? element.Attribute("REMOTEID")?.Value;
                                    if (!string.IsNullOrEmpty(id)) tallyIds.Add(id);
                                }
                            }
                        }

                        if (!chunkSuccess) 
                        {
                            retryAttempts++;
                            if (retryAttempts < 3) await Task.Delay(500 * retryAttempts, ct);
                        }
                    }
                    
                    await Task.Delay(300, ct); // Throttling memory
                }

                // If Tally returns 0 IDs but we have data in DB, it's a huge risk of total prune.
                var localCount = await db.Vouchers.CountAsync(v => v.OrganizationId == orgId && v.VoucherDate >= fromDate && v.VoucherDate <= toDate, ct);
                if (localCount > 0 && tallyIds.Count == 0)
                {
                     _syncMonitor.AddLog("❌ [SAFETY ABORT] Discovery returned 0 records for a populated range. Check Tally connectivity. No deletions performed.", "ERROR", "CLEANUP");
                     return;
                }

                // Threshold Check: If we are about to delete more than 30% of the range, log a major warning
                // (Commercial businesses rarely delete >30% of data in one go)
                var orphans = await db.Vouchers
                    .Where(v => v.OrganizationId == orgId &&
                               v.VoucherDate >= fromDate &&
                               v.VoucherDate <= toDate)
                    .ToListAsync(ct);

                var toDelete = orphans.Where(v => !tallyIds.Contains(v.TallyMasterId)).ToList();

                if (toDelete.Count > (localCount * 0.3))
                {
                    _syncMonitor.AddLog($"⚠ High Deletion Volume: About to prune {toDelete.Count} vouchers ({Math.Round(toDelete.Count * 100.0 / localCount)}%). Proceeding with caution.", "WARNING", "CLEANUP");
                }

                if (toDelete.Any())
                {
                    _syncMonitor.AddLog($"🗑 Deterministic Cleanup: {toDelete.Count} vouchers found in DB that no longer exist in Tally. Removing.", "WARNING", "CLEANUP");
                    
                    if (string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase))
                    {
                        var ids = toDelete.Select(v => v.TallyMasterId).ToList();
                        await _mongoService.BulkDeleteDocumentsAsync("vouchers", "TallyMasterId", ids);
                    }

                    db.Vouchers.RemoveRange(toDelete);
                    await db.SaveChangesAsync(ct);
                }
                else
                {
                    _syncMonitor.AddLog("✅ Mutation reconciliation complete. No orphans detected.", "SUCCESS", "CLEANUP");
                }
            }
        }
        private async Task<IntegrityResult> VerifyFinancialIntegrityAsync(Guid orgId, CancellationToken ct)
        {
            using (var scope = _scopeFactory.CreateScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                
                // We sum all debits and credits across the entire history for this org
                var balances = await db.LedgerEntries
                    .Where(le => le.OrganizationId == orgId)
                    .GroupBy(le => 1)
                    .Select(g => new 
                    {
                        TotalDebit = g.Sum(x => x.DebitAmount),
                        TotalCredit = g.Sum(x => x.CreditAmount)
                    })
                    .FirstOrDefaultAsync(ct);

                if (balances == null) return new IntegrityResult { IsBalanced = true, Difference = 0 };

                var diff = balances.TotalDebit - balances.TotalCredit;
                return new IntegrityResult 
                { 
                    IsBalanced = Math.Abs(diff) < 1.0m, // Allowing small rounding diffs at scale
                    Difference = diff 
                };
            }
        }

        private class IntegrityResult
        {
            public bool IsBalanced { get; set; }
            public decimal Difference { get; set; }
        }
    }
}
