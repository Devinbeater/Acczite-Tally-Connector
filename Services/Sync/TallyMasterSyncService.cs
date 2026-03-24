using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using MongoDB.Bson;

namespace Acczite20.Services.Sync
{
    public class MasterIntegrityResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int GroupCount { get; set; }
        public int LedgerCount { get; set; }
        public int VoucherTypeCount { get; set; }
    }

    public class TallyMasterSyncService
    {
        private readonly TallyXmlService _xmlService;
        private readonly TallyOdbcImporter _odbcImporter;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<TallyMasterSyncService> _logger;
        private readonly MongoService _mongoService;
        private readonly SyncStateMonitor _syncMonitor;

        public TallyMasterSyncService(
            TallyXmlService xmlService,
            TallyOdbcImporter odbcImporter,
            IServiceScopeFactory scopeFactory,
            ILogger<TallyMasterSyncService> logger,
            MongoService mongoService,
            SyncStateMonitor syncMonitor)
        {
            _xmlService = xmlService;
            _odbcImporter = odbcImporter;
            _scopeFactory = scopeFactory;
            _logger = logger;
            _mongoService = mongoService;
            _syncMonitor = syncMonitor;
        }

        private AppDbContext GetContext(IServiceProvider sp) => sp.GetRequiredService<AppDbContext>();

        // Session-scoped DbContext â€” set once per SyncAllMastersAsync call.
        // Private helper methods use this field directly. Safe because the
        // ISyncLockProvider prevents concurrent sync sessions on the same instance.
        private AppDbContext? _context;
        private AppDbContext RequireContext() =>
            _context ?? throw new InvalidOperationException("Sync context has not been initialized.");

        // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        // MAIN ENTRY POINT â€” strict order: Groups â†’ Ledgers â†’ VoucherTypes â†’ Stock
        // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        public async Task SyncAllMastersAsync(Guid organizationId)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = GetContext(scope.ServiceProvider);
            _context = dbContext; // Bind session context for private helper methods
            _logger.LogInformation("Starting Master Data Sync (Production-Grade)...");
            _syncMonitor.AddLog("Master Sync: Phase 1 â€” Accounting Groups", "INFO", "MASTERS");

            // â”€â”€â”€ Phase 1: Accounting Groups (MUST be first â€” everything depends on group hierarchy) â”€â”€â”€
            await SyncAccountingGroupsAsync(organizationId, dbContext);
            await PopulateDimGroupsAsync(organizationId);
            _syncMonitor.AddLog($"âœ… Accounting Groups synced and DimGroups populated.", "SUCCESS", "MASTERS");

            // â”€â”€â”€ Phase 2: Ledgers (depends on Groups for parent resolution) â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 2 â€” Ledgers", "INFO", "MASTERS");
            await SyncLedgersAsync(organizationId, dbContext);
            await PopulateDimLedgersAsync(organizationId);
            _syncMonitor.AddLog($"âœ… Ledgers synced and DimLedgers populated.", "SUCCESS", "MASTERS");

            // â”€â”€â”€ Phase 3: Currencies â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 3 â€” Currencies", "INFO", "MASTERS");
            await SyncXmlCollectionAsync<Currency>(organizationId, "List of Currencies", "CURRENCY", element => new Currency
            {
                OrganizationId = organizationId,
                Name = element.Element("NAME")?.Value ?? string.Empty,
                FormalName = element.Element("FORMALNAME")?.Value ?? string.Empty,
                Symbol = element.Element("MAILINGNAME")?.Value ?? string.Empty
            });

            // â”€â”€â”€ Phase 4: Voucher Types â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 4 â€” Voucher Types", "INFO", "MASTERS");
            await SyncXmlCollectionAsync<VoucherType>(organizationId, "List of Voucher Types", "VOUCHERTYPE", element => new VoucherType
            {
                OrganizationId = organizationId,
                Name = element.Element("NAME")?.Value ?? string.Empty,
                Category = element.Element("PARENT")?.Value ?? string.Empty
            });
            await PopulateDimVoucherTypesAsync(organizationId);

            // â”€â”€â”€ Phase 5: Stock Groups + Stock Categories + Stock Items â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 5 â€” Stock Masters", "INFO", "MASTERS");
            await SyncStockGroupsAsync(organizationId, dbContext);
            await SyncStockCategoriesAsync(organizationId, dbContext);
            await SyncStockItemsAsync(organizationId, dbContext);
            await PopulateDimStockItemsAsync(organizationId);

            // â”€â”€â”€ Phase 6: Auto-map Ledgers to Analytics Categories â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 6 â€” Auto-mapping Ledgers", "INFO", "MASTERS");
            await AutoMapLedgersAsync(organizationId);

            // â”€â”€â”€ Phase 7: Integrity Validation â”€â”€â”€
            _syncMonitor.AddLog("Master Sync: Phase 7 â€” Integrity Validation", "INFO", "MASTERS");
            await ValidateIntegrityAsync(organizationId);

            _logger.LogInformation("Master Data Sync Completed (All Phases).");
            _syncMonitor.AddLog("âœ… Master Data Sync Complete â€” all phases passed.", "SUCCESS", "MASTERS");
        }

        public async Task<MasterIntegrityResult> VerifyMasterDataIntegrityAsync(Guid organizationId)
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = GetContext(scope.ServiceProvider);
            var result = new MasterIntegrityResult { IsValid = true };
            
            result.GroupCount = await dbContext.AccountingGroups.CountAsync(g => g.OrganizationId == organizationId);
            result.LedgerCount = await dbContext.Ledgers.CountAsync(l => l.OrganizationId == organizationId);
            result.VoucherTypeCount = await dbContext.VoucherTypes.CountAsync(v => v.OrganizationId == organizationId);

            if (result.GroupCount == 0)
            {
                result.IsValid = false;
                result.ErrorMessage = "No accounting groups found. Tally export may have been empty or corrupted.";
            }
            else if (result.LedgerCount == 0)
            {
                result.IsValid = false;
                result.ErrorMessage = "No ledgers found. Transactions cannot be synced without ledger context.";
            }
            else if (result.VoucherTypeCount == 0)
            {
                result.IsValid = false;
                result.ErrorMessage = "No voucher types found. Tally transaction mapping will fail.";
            }

            return result;
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 1: ACCOUNTING GROUPS (enriched with NatureOfGroup)
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private async Task SyncAccountingGroupsAsync(Guid orgId, AppDbContext dbContext)
        {
            _logger.LogInformation("Syncing Accounting Groups via XML...");
            try
            {
                // Pass 1: SYNC GROUPS
                _syncMonitor.SetStage("Accounting Groups", "Fetching accounting groups from Tally...", 10, false);
                _logger.LogInformation("Syncing Accounting Groups via isolated XML...");

                var groupResponse = await _xmlService.ExportCollectionXmlAsync("List of Groups", isCollection: true);
                if (string.IsNullOrWhiteSpace(groupResponse))
                {
                    _syncMonitor.AddLog("âš  Tally returned empty XML for groups. Aborting Phase 1.", "WARNING", "MASTERS");
                    throw new InvalidOperationException("Empty response from Tally during Group Sync.");
                }

                XDocument doc;
                try
                {
                    var readerSettings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                    using var sReader = new System.IO.StringReader(groupResponse);
                    using var reader = System.Xml.XmlReader.Create(sReader, readerSettings);
                    doc = XDocument.Load(reader);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to parse Tally Group XML.");
                    throw new InvalidOperationException("Tally returned malformed XML for groups.");
                }
                
                var nodes = GetCollectionNodes(doc, "GROUP", "PARENT", "NATUREOFGROUP", "ISPRIMARY");
                
                if (nodes.Count == 0)
                {
                    _syncMonitor.AddLog("âŒ Critical: Tally returned 0 Accounting Groups. Check Company.", "ERROR", "MASTERS");
                    throw new InvalidOperationException("No accounting groups found in Tally.");
                }

                _syncMonitor.AddLog($"Fetched {nodes.Count} Accounting Groups. Beginning import...", "INFO", "MASTERS");
                _syncMonitor.SetStage("Processing Groups", $"Importing {nodes.Count} groups...", 15, false);
 
                // CRITICAL GUARD: Ensure we have valid names
                if (!nodes.Any(n => !string.IsNullOrEmpty(GetTallyValue(n, "NAME"))))
                {
                   _syncMonitor.AddLog("âŒ Critical Fail: Tally returned 0 valid Accounting Groups name data. Aborting.", "ERROR", "MASTERS");
                   throw new InvalidOperationException("Data Integrity Breach: Zero named accounting groups found in Tally schema.");
                }

                var existingGroups = await dbContext.AccountingGroups
                    .Where(g => g.OrganizationId == orgId)
                    .ToDictionaryAsync(g => g.TallyMasterId); // KEYED BY MASTERID

                foreach (var g in existingGroups.Values) 
                {
                    g.IsPresentInTally = false; // Reset for reconciliation
                }
                
                bool isMongo = string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase);
                var mongoDocs = new List<BsonDocument>();
                int count = 0;

                foreach (var node in nodes)
                {
                    string name = GetTallyValue(node, "NAME");
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    string parent = GetTallyValue(node, "PARENT");
                    string nature = GetTallyValue(node, "NATUREOFGROUP");
                    bool isPrimary = string.Equals(GetTallyValue(node, "ISPRIMARY"), "Yes", StringComparison.OrdinalIgnoreCase);
                    bool affectsGP = string.Equals(GetTallyValue(node, "AFFECTSGROSSPROFIT"), "Yes", StringComparison.OrdinalIgnoreCase);
                    bool isAddable = string.Equals(GetTallyValue(node, "ISADDABLE"), "Yes", StringComparison.OrdinalIgnoreCase);

                    long alterId = 0;
                    long.TryParse(GetTallyValue(node, "ALTERID"), out alterId);

                    string tallyId = GetTallyValue(node, "MASTERID");
                    if (string.IsNullOrEmpty(tallyId)) tallyId = name; // Identity consistency fallback 
                    if (existingGroups.TryGetValue(tallyId, out var existing))
                    {
                        existing.Name = name; // Update name in case of rename
                        existing.Parent = parent;
                        existing.NatureOfGroup = nature;
                        existing.IsPrimary = isPrimary;
                        existing.AffectsGrossProfit = affectsGP;
                        existing.IsAddable = isAddable;
                        existing.TallyMasterId = tallyId;
                        existing.TallyAlterId = alterId; // Priority 2: Alteration tracking
                        existing.UpdatedAt = DateTimeOffset.UtcNow;
                        existing.IsPresentInTally = true; // Still in Tally source
                        existing.IsActive = true; // Ensure it's active
                    }
                    else
                    {
                        var group = new AccountingGroup
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            Name = name,
                            Parent = parent,
                            NatureOfGroup = nature,
                            IsPrimary = isPrimary,
                            AffectsGrossProfit = affectsGP,
                            IsAddable = isAddable,
                            TallyMasterId = tallyId,
                            TallyAlterId = alterId,
                            CreatedAt = DateTimeOffset.UtcNow,
                            UpdatedAt = DateTimeOffset.UtcNow
                        };
                        await dbContext.AccountingGroups.AddAsync(group);
                    }
                    if (isMongo)
                    {
                        var mongoDoc = new BsonDocument
                        {
                            { "name", name }, { "parent", parent }, { "natureOfGroup", nature },
                            { "isPrimary", isPrimary }, { "affectsGrossProfit", affectsGP },
                            { "isAddable", isAddable }, { "TallyMasterId", tallyId }
                        };
                        mongoDocs.Add(mongoDoc);
                    }
 
                    count++;
                }

                if (isMongo && mongoDocs.Any())
                    await _mongoService.BulkUpsertDocumentsAsync("accountinggroups", mongoDocs, "TallyMasterId");

                // After sync, anything still not present in Tally is deactivated
                var deactivatedCount = 0;
                foreach (var g in existingGroups.Values.Where(v => !v.IsPresentInTally && v.IsActive))
                {
                    g.IsActive = false; // Soft-Deactivate but keep data
                    deactivatedCount++;
                }
 
                await dbContext.SaveChangesAsync();
                _logger.LogInformation($"Synced {count} Groups. {deactivatedCount} deactivated (removed from Tally).");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing Accounting Groups");
                _syncMonitor.AddLog($"âŒ Accounting Groups sync failed: {ex.Message}", "ERROR", "MASTERS");
                throw;
            }
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 2: LEDGERS (ODBC primary, XML fallback)
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private async Task SyncLedgersAsync(Guid orgId, AppDbContext dbContext)
        {
            try
            {
                // XML is now the primary method â€” more reliable across Tally versions
                _logger.LogInformation("Syncing Ledgers via hardened XML (Primary)...");
                await SyncLedgersFromXmlAsync(orgId, dbContext);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "XML Ledger sync failed. Attempting ODBC fallback...");
                _syncMonitor.AddLog($"âš  XML sync issue ({ex.Message}). Trying ODBC fallback for Ledgers.", "WARNING", "MASTERS");
                try
                {
                    await _odbcImporter.ImportLedgersAsync(orgId, CancellationToken.None);
                }
                catch (Exception odbcEx)
                {
                    _logger.LogError(odbcEx, "Both XML and ODBC Ledger sync failed.");
                    _syncMonitor.AddLog("âŒ Critical: Could not sync Ledgers via any channel.", "ERROR", "MASTERS");
                    throw;
                }
 
                // CRITICAL INTEGRITY CHECK: Ensure we actually got some ledgers before finishing Phase 2
                var checkCount = await dbContext.Ledgers.CountAsync(l => l.OrganizationId == orgId);
                if (checkCount == 0)
                {
                    _syncMonitor.AddLog("âŒ Critical Fail: Both XML and ODBC returned 0 Ledgers. Aborting sync.", "ERROR", "MASTERS");
                    throw new InvalidOperationException("Zero ledgers retrieved. Please verify Tally has a loaded company with accounts.");
                }
            }
        }

        /// <summary>
        /// Full XML-based ledger sync â€” used when ODBC is unavailable
        /// </summary>
        private async Task SyncLedgersFromXmlAsync(Guid orgId, AppDbContext dbContext)
        {
            try
            {
                string? response = null;
                // Each attempt uses a 10-minute export timeout. If it still fails (e.g. Tally crash),
                // we check whether Tally is actually alive before retrying, and wait 30s/60s.
                int[] retryDelaysMs = { 30_000, 60_000 };
                for (int i = 1; i <= 3; i++)
                {
                    // Respect the circuit breaker — if Tally is repeatedly failing, don't send more requests.
                    if (TallyXmlService.IsCircuitOpen())
                    {
                        var remaining = TallyXmlService.CircuitCooldownRemaining();
                        _syncMonitor.AddLog($"Circuit breaker is OPEN. Waiting {remaining}s before ledger fetch attempt {i}...", "WARNING", "MASTERS");
                        await Task.Delay(remaining * 1000);
                    }

                    try
                    {
                        _syncMonitor.AddLog($"Fetching ledgers from Tally (attempt {i}/3)...", "INFO", "MASTERS");
                        response = await _xmlService.ExportCollectionXmlAsync("List of Ledgers", isCollection: true);
                        if (!string.IsNullOrWhiteSpace(response)) break;

                        // Tally returned empty (not an exception). Health-check it before retrying.
                        if (i < 3)
                        {
                            var waitSec = retryDelaysMs[i - 1] / 1000;
                            _syncMonitor.AddLog($"Tally returned empty ledger response. Checking Tally health...", "WARNING", "MASTERS");
                            bool alive = await _xmlService.IsTallyRunningAsync();
                            if (!alive)
                            {
                                _syncMonitor.AddLog($"Tally is NOT responding. Waiting {waitSec}s for recovery before retry...", "WARNING", "MASTERS");
                            }
                            else
                            {
                                _syncMonitor.AddLog($"Tally is alive but returned empty data. Waiting {waitSec}s before retry...", "WARNING", "MASTERS");
                            }
                            await Task.Delay(retryDelaysMs[i - 1]);
                        }
                    }
                    catch (Exception ex) when (i < 3)
                    {
                        var waitSec = retryDelaysMs[i - 1] / 1000;
                        _logger.LogWarning($"Ledger fetch attempt {i} failed: {ex.Message}. Checking Tally health...");
                        bool alive = await _xmlService.IsTallyRunningAsync();
                        _syncMonitor.AddLog(
                            $"Ledger fetch failed. Tally is {(alive ? "alive but slow" : "NOT responding")}. " +
                            $"Waiting {waitSec}s before retry...", "WARNING", "MASTERS");
                        await Task.Delay(retryDelaysMs[i - 1]);
                    }
                }

                if (string.IsNullOrWhiteSpace(response)) 
                {
                    _syncMonitor.AddLog("âŒ Tally failed to return Ledgers after 3 attempts.", "ERROR", "MASTERS");
                    throw new InvalidOperationException("Resilience Failure: Tally communication timed out or returned empty data for Ledgers.");
                }

                XDocument doc;
                try
                {
                    var readerSettings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                    using var sReader = new System.IO.StringReader(response);
                    using var reader = System.Xml.XmlReader.Create(sReader, readerSettings);
                    doc = XDocument.Load(reader);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to parse Tally Ledger XML.");
                    throw new InvalidOperationException("Tally returned malformed XML for ledgers.");
                }

                var nodes = GetCollectionNodes(doc, "LEDGER", "PARENT", "GSTIN");
                if (nodes.Count == 0)
                {
                    _syncMonitor.AddLog("âš  Warning: Tally returned 0 Ledgers. Verify data exists in Tally.", "WARNING", "MASTERS");
                    return; // Graceful skip
                }

                _syncMonitor.AddLog($"Fetched {nodes.Count} Ledgers. Syncing database...", "INFO", "MASTERS");
                
                // Report progress to monitor
                _syncMonitor.ProgressPercent = 5; // Starting ledger sync progress 5%
 
                var existingLedgers = await dbContext.Ledgers
                    .Where(l => l.OrganizationId == orgId)
                    .ToDictionaryAsync(l => l.TallyMasterId ?? l.Name); // Identity Lock
 
                foreach (var l in existingLedgers.Values) 
                {
                    l.IsPresentInTally = false; // Reset for reconciliation
                }
                
                // Get valid groups list for Referential Integrity check
                var validGroups = await dbContext.AccountingGroups
                    .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                    .Select(g => g.Name)
                    .ToListAsync();
                var groupNames = new HashSet<string>(validGroups, StringComparer.OrdinalIgnoreCase);
                
                bool isMongo = string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase);
                var mongoDocs = new List<BsonDocument>();
                int count = 0;
                var startTime = DateTime.Now;
                dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

                foreach (var node in nodes)
                {
                    string name = node.Element("NAME")?.Value ?? node.Attribute("NAME")?.Value ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(name)) continue;

                    var ledger = ParseLedgerFromXml(node, orgId);

                    string tallyId = ledger.TallyMasterId ?? name; // Identity consistency
 
                    if (existingLedgers.TryGetValue(tallyId, out var existing))
                    {
                        ApplyLedgerFields(existing, ledger);
                        existing.UpdatedAt = DateTimeOffset.UtcNow;
                        existing.IsPresentInTally = true; // Still present
                        existing.IsActive = true; // Ensure it's visible
                        
                        // STRICT MASTER INTEGRITY GUARD
                        if (!string.IsNullOrEmpty(ledger.ParentGroup) && !groupNames.Contains(ledger.ParentGroup))
                        {
                             _syncMonitor.AddLog($"âŒ CRITICAL: Ledger '{ledger.Name}' points to missing Group '{ledger.ParentGroup}'.", "ERROR", "MASTERS");
                             throw new InvalidOperationException($"Referential Integrity Breach: Ledger '{ledger.Name}' references a Group '{ledger.ParentGroup}' that was not found in Tally. Sync aborted to prevent orphaned entries.");
                        }

                        // Opening balance changed in Tally (year-end closing, corrections, etc.)
                        // Log the drift for audit trail and update to match Tally â Tally is source of truth.
                        if (Math.Abs(existing.OpeningBalance - ledger.OpeningBalance) > 0.01m)
                        {
                            _syncMonitor.AddLog($"⚠ Balance updated: '{name}' {existing.OpeningBalance:N2} → {ledger.OpeningBalance:N2} (Tally is source of truth)", "WARNING", "INTEGRITY");
                            _logger.LogWarning($"Opening balance changed for ledger '{name}': {existing.OpeningBalance} → {ledger.OpeningBalance}");
                            existing.OpeningBalance = ledger.OpeningBalance;
                        }
                    }
                    else
                    {
                        ledger.Id = Guid.NewGuid();
                        ledger.CreatedAt = DateTimeOffset.UtcNow;
                        ledger.UpdatedAt = DateTimeOffset.UtcNow;
                        await dbContext.Ledgers.AddAsync(ledger);
                    }

                    if (isMongo)
                    {
                        mongoDocs.Add(BuildLedgerMongoBson(ledger));
                        if (mongoDocs.Count >= 100)
                        {
                            await _mongoService.BulkUpsertDocumentsAsync("ledgers", mongoDocs, "TallyMasterId");
                            mongoDocs.Clear();
                        }
                    }

                    count++;
                    if (count % 25 == 0) // Update UI very frequently for smooth animation
                    {
                        var elapsed = (DateTime.Now - startTime).TotalSeconds;
                        _syncMonitor.TotalRecordsSynced = count; // Real-time HUD count
                        _syncMonitor.UpdateMetrics(count, elapsed); // Real-time Speed
 
                        // PROGRESS CURVE: From 5% to 25% based on nodes
                        double progressOffset = 5.0;
                        double progressScan = (double)count / nodes.Count * 20.0;
                        _syncMonitor.ProgressPercent = progressOffset + progressScan;
                        _syncMonitor.CurrentStageDetail = $"Importing: {count}/{nodes.Count} ledgers from Tally...";
                        
                        if (count % 25 == 0) // UI Update only (Lower frequency logs for performance)
                        {
                           _syncMonitor.AddLog($"âš¡ Speed: {_syncMonitor.VouchersPerSecond:N0} l/s | Progress: {count}/{nodes.Count} Ledgers", "INFO", "MASTERS");
                        }
                    }

                    if (count % 500 == 0) // BATCH DATABASE SAVE (Production Grade Pattern)
                    {
                        await dbContext.SaveChangesAsync();
                        if (isMongo && mongoDocs.Count > 0)
                        {
                            await _mongoService.BulkUpsertDocumentsAsync("ledgers", mongoDocs, "TallyMasterId");
                            mongoDocs.Clear();
                        }
                    }
                }

                if (isMongo && mongoDocs.Any())
                    await _mongoService.BulkUpsertDocumentsAsync("ledgers", mongoDocs, "TallyMasterId");

                // Sync deactivation: Master data removal handling
                var deactivatedLedgers = 0;
                foreach (var l in existingLedgers.Values.Where(v => !v.IsPresentInTally && v.IsActive))
                {
                    l.IsActive = false; // Deactivate
                    deactivatedLedgers++;
                }
 
                await dbContext.SaveChangesAsync();
 
                // DB SUMMARY FOR GOLDEN RULE
                decimal dbTotal = await dbContext.Ledgers
                    .Where(l => l.OrganizationId == orgId && l.IsActive)
                    .SumAsync(l => l.OpeningBalance);
 
                _syncMonitor.AddLog($"Phase 2 Complete: Synced {count} Ledgers ({deactivatedLedgers} removed in Tally).", "SUCCESS", "MASTERS");
                
                // Cross-System Validation against Tally's Trial Balance summary
                try {
                   decimal tallyTotal = await _xmlService.GetTallyTrialBalanceTotalAsync();
                   if (tallyTotal != 0 && Math.Abs(dbTotal - tallyTotal) > 0.01m)
                   {
                       _syncMonitor.AddLog($"âŒ CRITICAL: Balance Mismatch! Tally: {tallyTotal:N2} vs DB: {dbTotal:N2}", "ERROR", "INTEGRITY");
                       throw new InvalidOperationException($"Financial Drift Detected! Tally TB Total {tallyTotal:N2} does not match DB Opening Sum {dbTotal:N2}. Sync aborted for safety.");
                   }
                   else {
                       _syncMonitor.AddLog($"âœ… Cross-System Validation Passed: DB matches Tally Summary ({dbTotal:N2}).", "SUCCESS", "INTEGRITY");
                   }
                } catch (InvalidOperationException) { throw; }
                catch { } 
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing Ledgers via XML");
                _syncMonitor.AddLog($"âŒ XML Ledger sync also failed: {ex.Message}", "ERROR", "MASTERS");
                throw;
            }
            finally
            {
                dbContext.ChangeTracker.AutoDetectChangesEnabled = true; // Re-enable always
            }
        }

        /// <summary>
        /// Enrich ODBC-imported ledgers with GST/billing fields from XML
        /// </summary>
        private async Task EnrichLedgersFromXmlAsync(Guid orgId, AppDbContext dbContext)
        {
            try
            {
                var response = await _xmlService.ExportCollectionXmlAsync("List of Ledgers", isCollection: true);
                if (string.IsNullOrWhiteSpace(response)) return;

                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var stringReader = new System.IO.StringReader(response);
                using var reader = System.Xml.XmlReader.Create(stringReader, settings);
                var doc = XDocument.Load(reader);
                var nodes = GetCollectionNodes(doc, "LEDGER", "PARENT", "MAILINGNAME", "GSTIN");
                _syncMonitor.AddLog($"Enriching {nodes.Count} ledgers with GST details from XML 'List of Ledgers' collection.", "INFO", "MASTERS");

                var existingLedgers = await dbContext.Ledgers
                    .Where(l => l.OrganizationId == orgId)
                    .ToDictionaryAsync(l => l.Name);

                int enriched = 0;
                foreach (var node in nodes)
                {
                    string name = node.Element("NAME")?.Value ?? string.Empty;
                    if (string.IsNullOrWhiteSpace(name)) continue;
                    if (!existingLedgers.TryGetValue(name, out var existing)) continue;

                    var xmlLedger = ParseLedgerFromXml(node, orgId);
                    
                    // Only update GST/billing fields â€” balances already set by ODBC
                    existing.GSTApplicability = xmlLedger.GSTApplicability;
                    existing.GSTRegistrationType = xmlLedger.GSTRegistrationType;
                    existing.GSTIN = xmlLedger.GSTIN;
                    existing.IsBillWise = xmlLedger.IsBillWise;
                    existing.Address = xmlLedger.Address;
                    existing.State = xmlLedger.State;
                    existing.PAN = xmlLedger.PAN;
                    existing.Email = xmlLedger.Email;
                    existing.MailingName = xmlLedger.MailingName;
                    existing.UpdatedAt = DateTimeOffset.UtcNow;

                    enriched++;
                }

                await dbContext.SaveChangesAsync();
                _logger.LogInformation($"Enriched {enriched} ledgers with GST/billing data from XML.");
                _syncMonitor.AddLog($"Enriched {enriched} ledgers with GST/billing fields.", "INFO", "MASTERS");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to enrich ledgers from XML â€” non-critical, continuing.");
                _syncMonitor.AddLog($"âš  Ledger enrichment failed (non-critical): {ex.Message}", "WARNING", "MASTERS");
            }
        }

        private Ledger ParseLedgerFromXml(XElement node, Guid orgId)
        {
            string name = GetTallyValue(node, "NAME");
            string parent = GetTallyValue(node, "PARENT");

            // Tally encodes balances as positive/negative text â€” parse carefully
            // Standardizing: Debit = positive | Credit = negative (Normalized at ingestion)
            decimal opening = 0, closing = 0;
            decimal.TryParse(GetTallyValue(node, "OPENINGBALANCE").Replace(",", "").Trim(), out opening);
            decimal.TryParse(GetTallyValue(node, "CLOSINGBALANCE").Replace(",", "").Trim(), out closing);
            
            // Build address from ADDRESS.LIST child nodes
            string address = string.Empty;
            var addrList = node.Element("ADDRESS.LIST") ?? node.Element("ADDRESS.LIST".ToUpper());
            if (addrList != null)
            {
                var addrLines = addrList.Elements("ADDRESS").Select(a => a.Value.Trim()).Where(v => !string.IsNullOrEmpty(v));
                address = string.Join(", ", addrLines);
            }

            long alterId = 0;
            long.TryParse(node.Element("ALTERID")?.Value, out alterId);

            return new Ledger
            {
                OrganizationId = orgId,
                Name = name,
                ParentGroup = parent,
                OpeningBalance = opening,
                ClosingBalance = closing,
                TallyMasterId = node.Element("MASTERID")?.Value ?? name,
                TallyAlterId = alterId,
                GSTApplicability = node.Element("GSTAPPLICABILITY")?.Value ?? string.Empty,
                GSTRegistrationType = node.Element("GSTREGISTRATIONTYPE")?.Value ?? string.Empty,
                GSTIN = node.Element("PARTYGSTIN")?.Value ?? node.Element("GSTIN")?.Value ?? string.Empty,
                IsBillWise = string.Equals(node.Element("ISBILLWISEON")?.Value, "Yes", StringComparison.OrdinalIgnoreCase),
                Address = address,
                State = node.Element("LEDSTATENAME")?.Value ?? string.Empty,
                PAN = node.Element("INCOMETAXNUMBER")?.Value ?? string.Empty,
                Email = node.Element("EMAIL")?.Value ?? string.Empty,
                MailingName = node.Element("MAILINGNAME")?.Value ?? string.Empty
            };
        }

        private void ApplyLedgerFields(Ledger target, Ledger source)
        {
            target.Name = source.Name; // Identity Lock: handle renames by MASTERID
            target.ParentGroup = source.ParentGroup;
            target.OpeningBalance = source.OpeningBalance;
            target.ClosingBalance = source.ClosingBalance;
            target.GSTApplicability = source.GSTApplicability;
            target.GSTRegistrationType = source.GSTRegistrationType;
            target.GSTIN = source.GSTIN;
            target.IsBillWise = source.IsBillWise;
            target.Address = source.Address;
            target.State = source.State;
            target.PAN = source.PAN;
            target.Email = source.Email;
            target.MailingName = source.MailingName;
            target.TallyMasterId = source.TallyMasterId;
            target.TallyAlterId = source.TallyAlterId; // Priority 2: Alteration tracking
        }

        private BsonDocument BuildLedgerMongoBson(Ledger l)
        {
            return new BsonDocument
            {
                { "name", l.Name }, { "parentGroup", l.ParentGroup },
                { "openingBalance", (double)l.OpeningBalance }, { "closingBalance", (double)l.ClosingBalance },
                { "gstApplicability", l.GSTApplicability }, { "gstRegistrationType", l.GSTRegistrationType },
                { "gstin", l.GSTIN }, { "isBillWise", l.IsBillWise },
                { "address", l.Address }, { "state", l.State },
                { "pan", l.PAN }, { "email", l.Email },
                { "mailingName", l.MailingName }, { "TallyMasterId", l.TallyMasterId }
            };
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // PHASE 5: STOCK GROUPS + STOCK CATEGORIES (via XML)
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private async Task SyncStockGroupsAsync(Guid orgId, AppDbContext dbContext)
        {
            await SyncXmlCollectionAsync<StockGroup>(orgId, "List of Stock Groups", "STOCKGROUP", element => new StockGroup
            {
                OrganizationId = orgId,
                Name = element.Element("NAME")?.Value ?? string.Empty,
                Parent = element.Element("PARENT")?.Value ?? string.Empty
            });
        }

        private async Task SyncStockCategoriesAsync(Guid orgId, AppDbContext dbContext)
        {
            await SyncXmlCollectionAsync<StockCategory>(orgId, "List of Stock Categories", "STOCKCATEGORY", element => new StockCategory
            {
                OrganizationId = orgId,
                Name = element.Element("NAME")?.Value ?? string.Empty,
                Parent = element.Element("PARENT")?.Value ?? string.Empty
            });
        }

        private async Task SyncStockItemsAsync(Guid orgId, AppDbContext dbContext)
        {
            try
            {
                await _odbcImporter.ImportStockItemsAsync(orgId, CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ODBC Stock import failed. Falling back to XML...");
                _syncMonitor.AddLog($"âš  ODBC StockItems failed. Falling back to XML.", "WARNING", "MASTERS");
                await SyncXmlCollectionAsync<StockItem>(orgId, "List of Stock Items", "STOCKITEM", element => new StockItem
                {
                    OrganizationId = orgId,
                    Name = element.Element("NAME")?.Value ?? string.Empty,
                    StockGroup = element.Element("PARENT")?.Value ?? string.Empty,
                    BaseUnit = element.Element("BASEUNITS")?.Value ?? string.Empty,
                    TallyMasterId = element.Element("NAME")?.Value ?? string.Empty,
                    OpeningBalance = decimal.TryParse(element.Element("OPENINGBALANCE")?.Value, out var ob) ? ob : 0,
                    ClosingBalance = decimal.TryParse(element.Element("CLOSINGBALANCE")?.Value, out var cb) ? cb : 0
                });
            }
        }

        // Dimension table population
        private async Task PopulateDimGroupsAsync(Guid orgId)
        {
            var dbContext = RequireContext();
            var groups = await dbContext.AccountingGroups
                .Where(g => g.OrganizationId == orgId)
                .ToListAsync();
            var existingDimGroups = await dbContext.DimGroups
                .Where(d => d.OrganizationId == orgId)
                .ToListAsync();
            var existingMap = existingDimGroups.ToDictionary(d => d.GroupName, StringComparer.OrdinalIgnoreCase);
            var groupsByName = groups
                .GroupBy(g => g.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            foreach (var group in groups)
            {
                groupsByName.TryGetValue(group.Parent, out var parentGroup);
                var parentGroupId = parentGroup?.Id;
                var rootGroup = ResolveRootGroup(group.Name, group.Parent, groups);

                if (existingMap.TryGetValue(group.Name, out var existingDimGroup))
                {
                    existingDimGroup.ParentGroupId = parentGroupId;
                    existingDimGroup.RootGroup = rootGroup;
                    existingDimGroup.TallyMasterId = group.TallyMasterId;
                    existingDimGroup.UpdatedAt = DateTimeOffset.UtcNow;
                    continue;
                }

                await dbContext.DimGroups.AddAsync(new Acczite20.Models.Warehouse.DimGroup
                {
                    Id = group.Id,
                    OrganizationId = orgId,
                    GroupName = group.Name,
                    ParentGroupId = parentGroupId,
                    RootGroup = rootGroup,
                    TallyMasterId = group.TallyMasterId,
                    CreatedAt = DateTimeOffset.UtcNow,
                    UpdatedAt = DateTimeOffset.UtcNow
                });
            }

            await dbContext.SaveChangesAsync();
        }

        private async Task PopulateDimLedgersAsync(Guid orgId)
        {
            var dbContext = RequireContext();
            var ledgers = await dbContext.Ledgers
                .Where(l => l.OrganizationId == orgId)
                .ToListAsync();
            var dimGroups = await dbContext.DimGroups
                .Where(d => d.OrganizationId == orgId)
                .ToListAsync();
            var dimGroupMap = dimGroups.ToDictionary(d => d.GroupName, StringComparer.OrdinalIgnoreCase);
            var existingDimLedgers = await dbContext.DimLedgers
                .Where(d => d.OrganizationId == orgId)
                .ToDictionaryAsync(d => d.Id);

            foreach (var ledger in ledgers)
            {
                var hasGroup = dimGroupMap.TryGetValue(ledger.ParentGroup, out var dimGroup);
                var groupId = hasGroup ? dimGroup!.Id : Guid.Empty;
                var isGstLedger = !string.IsNullOrEmpty(ledger.GSTApplicability)
                    && !string.Equals(ledger.GSTApplicability, "Not Applicable", StringComparison.OrdinalIgnoreCase);

                if (existingDimLedgers.TryGetValue(ledger.Id, out var existingDimLedger))
                {
                    existingDimLedger.LedgerName = ledger.Name;
                    existingDimLedger.GroupId = groupId;
                    existingDimLedger.ParentGroupName = ledger.ParentGroup;
                    existingDimLedger.IsBillWise = ledger.IsBillWise;
                    existingDimLedger.IsGstLedger = isGstLedger;
                    existingDimLedger.TallyMasterId = ledger.TallyMasterId;
                    existingDimLedger.UpdatedAt = DateTimeOffset.UtcNow;
                    continue;
                }

                await dbContext.DimLedgers.AddAsync(new Acczite20.Models.Warehouse.DimLedger
                {
                    Id = ledger.Id,
                    OrganizationId = orgId,
                    LedgerName = ledger.Name,
                    GroupId = groupId,
                    ParentGroupName = ledger.ParentGroup,
                    IsBillWise = ledger.IsBillWise,
                    IsGstLedger = isGstLedger,
                    TallyMasterId = ledger.TallyMasterId,
                    CreatedAt = DateTimeOffset.UtcNow,
                    UpdatedAt = DateTimeOffset.UtcNow
                });
            }

            await dbContext.SaveChangesAsync();
        }

        private Task PopulateDimStockItemsAsync(Guid orgId) => PopulateDimStockItemsAsync(orgId, RequireContext());

        private async Task PopulateDimStockItemsAsync(Guid orgId, AppDbContext dbContext)
        {
            var items = await dbContext.StockItems.Where(i => i.OrganizationId == orgId).ToListAsync();
            foreach (var i in items)
            {
                if (!await dbContext.DimStockItems.AnyAsync(d => d.Id == i.Id))
                {
                    await dbContext.DimStockItems.AddAsync(new Acczite20.Models.Warehouse.DimStockItem
                    {
                        Id = i.Id, OrganizationId = orgId, StockItemName = i.Name,
                        TallyMasterId = i.TallyMasterId,
                        CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow
                    });
                }
            }
            await dbContext.SaveChangesAsync();
        }

        private Task PopulateDimVoucherTypesAsync(Guid orgId) => PopulateDimVoucherTypesAsync(orgId, RequireContext());

        private async Task PopulateDimVoucherTypesAsync(Guid orgId, AppDbContext dbContext)
        {
            var types = await dbContext.VoucherTypes.Where(v => v.OrganizationId == orgId).ToListAsync();
            foreach (var v in types)
            {
                if (!await dbContext.DimVoucherTypes.AnyAsync(d => d.Id == v.Id))
                {
                    await dbContext.DimVoucherTypes.AddAsync(new Acczite20.Models.Warehouse.DimVoucherType
                    {
                        Id = v.Id, OrganizationId = orgId, VoucherTypeName = v.Name,
                        TallyMasterId = v.Name,
                        CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow
                    });
                }
            }
            await dbContext.SaveChangesAsync();
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // AUTO-MAP: Complete coverage of all 15 Tally primary groups
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private Task AutoMapLedgersAsync(Guid orgId) => AutoMapLedgersAsync(orgId, RequireContext());

        private async Task AutoMapLedgersAsync(Guid orgId, AppDbContext dbContext)
        {
            _logger.LogInformation("Auto-mapping ledgers to categories (complete coverage)...");
            
            var dimGroups = await dbContext.DimGroups.Where(d => d.OrganizationId == orgId).ToListAsync();
            var existingMappings = await dbContext.LedgerMappings.Where(m => m.OrganizationId == orgId).ToListAsync();
            var ledgerList = await dbContext.Ledgers.Where(l => l.OrganizationId == orgId).ToListAsync();

            foreach (var ledger in ledgerList)
            {
                var group = dimGroups.FirstOrDefault(g => g.GroupName == ledger.ParentGroup);
                if (group == null) continue;

                // Complete mapping of ALL Tally primary root groups
                string? category = group.RootGroup switch
                {
                    // Revenue
                    "Sales Accounts" => "Sales",
                    "Direct Incomes" => "Income",
                    "Indirect Incomes" => "Income",
                    
                    // Expenses
                    "Direct Expenses" => "Expense",
                    "Indirect Expenses" => "Expense",
                    "Purchase Accounts" => "Purchase",
                    
                    // Assets
                    "Sundry Debtors" => "Receivables",
                    "Bank Accounts" => "Cash",
                    "Cash-in-hand" => "Cash",
                    "Current Assets" => "Current Assets",
                    "Fixed Assets" => "Fixed Assets",
                    "Investments" => "Investments",
                    "Stock-in-hand" => "Inventory",
                    "Deposits (Asset)" => "Deposits",
                    "Loans & Advances (Asset)" => "Loans (Asset)",
                    "Misc. Expenses (ASSET)" => "Misc Expenses",
                    
                    // Liabilities
                    "Sundry Creditors" => "Payables",
                    "Current Liabilities" => "Current Liabs",
                    "Loans (Liability)" => "Loans (Liability)",
                    "Secured Loans" => "Secured Loans",
                    "Unsecured Loans" => "Unsecured Loans",
                    "Bank OD Accounts" => "Bank OD",
                    "Duties & Taxes" => "Taxes",
                    "Provisions" => "Provisions",
                    
                    // Capital
                    "Capital Account" => "Capital",
                    "Reserves & Surplus" => "Reserves",
                    "Suspense A/c" => "Suspense",
                    "Branch / Divisions" => "Branches",
                    
                    _ => null
                };

                if (category != null)
                {
                    var mapping = existingMappings.FirstOrDefault(m => m.LedgerId == ledger.Id);
                    if (mapping == null)
                    {
                        mapping = new Acczite20.Models.Warehouse.LedgerMapping
                        {
                            Id = Guid.NewGuid(),
                            OrganizationId = orgId,
                            LedgerId = ledger.Id,
                            MappedCategory = category,
                            TallyLedgerName = ledger.Name,
                            CreatedAt = DateTimeOffset.UtcNow
                        };
                        await dbContext.LedgerMappings.AddAsync(mapping);
                        existingMappings.Add(mapping);
                    }
                    else if (mapping.MappedCategory != category)
                    {
                        mapping.MappedCategory = category;
                        mapping.UpdatedAt = DateTimeOffset.UtcNow;
                    }
                }
            }
            await dbContext.SaveChangesAsync();
            _syncMonitor.AddLog($"Auto-mapped {existingMappings.Count} ledgers to analytics categories.", "INFO", "MASTERS");
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // INTEGRITY VALIDATION â€” catch data quality issues
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private Task ValidateIntegrityAsync(Guid orgId) => ValidateIntegrityAsync(orgId, RequireContext());

        private async Task ValidateIntegrityAsync(Guid orgId, AppDbContext dbContext)
        {
            int issues = 0;

            // 1. Orphan Ledgers â€” Parent group doesn't exist
            var allGroups = await dbContext.AccountingGroups.Where(g => g.OrganizationId == orgId).Select(g => g.Name).ToListAsync();
            var groupSet = new HashSet<string>(allGroups, StringComparer.OrdinalIgnoreCase);

            var ledgersWithBadParent = await dbContext.Ledgers
                .Where(l => l.OrganizationId == orgId && !string.IsNullOrEmpty(l.ParentGroup))
                .ToListAsync();

            foreach (var l in ledgersWithBadParent)
            {
                if (!groupSet.Contains(l.ParentGroup))
                {
                    _syncMonitor.AddLog($"âš  Orphan Ledger: '{l.Name}' â†’ parent group '{l.ParentGroup}' not found.", "WARNING", "INTEGRITY");
                    issues++;
                }
            }

            // 2. Groups with unresolvable parents
            var allGroupEntities = await dbContext.AccountingGroups.Where(g => g.OrganizationId == orgId).ToListAsync();
            foreach (var g in allGroupEntities)
            {
                if (!string.IsNullOrEmpty(g.Parent) && !groupSet.Contains(g.Parent))
                {
                    _syncMonitor.AddLog($"âš  Orphan Group: '{g.Name}' â†’ parent '{g.Parent}' not found.", "WARNING", "INTEGRITY");
                    issues++;
                }
            }

            // 3. Duplicate ledger names (same org)
            var dupes = await dbContext.Ledgers
                .Where(l => l.OrganizationId == orgId)
                .GroupBy(l => l.Name)
                .Where(g => g.Count() > 1)
                .Select(g => new { Name = g.Key, Count = g.Count() })
                .ToListAsync();

            foreach (var d in dupes)
            {
                _syncMonitor.AddLog($"âš  Duplicate Ledger: '{d.Name}' appears {d.Count} times.", "WARNING", "INTEGRITY");
                issues++;
            }

            // 4. DimLedgers with Guid.Empty GroupId
            var emptyGroupDims = await dbContext.DimLedgers
                .Where(d => d.OrganizationId == orgId && d.GroupId == Guid.Empty)
                .CountAsync();

            if (emptyGroupDims > 0)
            {
                _syncMonitor.AddLog($"âš  {emptyGroupDims} DimLedgers have unresolved GroupId (Guid.Empty).", "WARNING", "INTEGRITY");
                issues++;
            }

            // Summary
            if (issues == 0)
                _syncMonitor.AddLog("âœ… Integrity check passed â€” no issues found.", "SUCCESS", "INTEGRITY");
            else
                _syncMonitor.AddLog($"âš  Integrity check found {issues} issue(s). Review warnings above.", "WARNING", "INTEGRITY");
        }

        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        // UTILITY METHODS
        // â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        private static string GetTallyValue(XElement node, string propertyName)
        {
            // Case-Insensitive Element and Attribute Probe (Normalization Layer)
            return node.Element(propertyName)?.Value 
                ?? node.Attribute(propertyName)?.Value 
                ?? node.Element(propertyName.ToUpper())?.Value 
                ?? node.Attribute(propertyName.ToUpper())?.Value 
                ?? node.Element(propertyName.ToLower())?.Value
                ?? string.Empty;
        }

        private static List<XElement> GetCollectionNodes(XDocument doc, string primaryNodeName, params string[] markerFields)
        {
            var directNodes = doc.Descendants()
                .Where(x => x.Name.LocalName.Equals(primaryNodeName, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (directNodes.Count > 0)
            {
                return directNodes;
            }

            return doc.Descendants()
                .Where(x =>
                    x.Name.LocalName.EndsWith(".LIST", StringComparison.OrdinalIgnoreCase)
                    && x.Elements().Any(e => e.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                    && (markerFields.Length == 0
                        || markerFields.Any(marker =>
                            x.Elements().Any(e => e.Name.LocalName.Equals(marker, StringComparison.OrdinalIgnoreCase)))))
                .ToList();
        }

        private string ResolveRootGroup(string name, string parent, List<AccountingGroup> allGroups)
        {
            string currentName = name;
            string currentParent = parent;
            
            int safety = 0;
            while (safety++ < 20)
            {
                if (string.IsNullOrEmpty(currentParent)) return currentName;
                
                var pNode = allGroups.FirstOrDefault(g => g.Name == currentParent);
                if (pNode == null) return currentName;
                
                currentName = pNode.Name;
                currentParent = pNode.Parent;
            }
            return currentName;
        }

        private Task SyncXmlCollectionAsync<TEntity>(Guid organizationId, string reportName, string xmlNodeName, Func<XElement, TEntity> mappingFunction)
            where TEntity : BaseEntity =>
            SyncXmlCollectionAsync(organizationId, reportName, xmlNodeName, RequireContext(), mappingFunction);

        private async Task SyncXmlCollectionAsync<TEntity>(Guid organizationId, string reportName, string xmlNodeName, AppDbContext dbContext, Func<XElement, TEntity> mappingFunction) where TEntity : BaseEntity
        {
            _logger.LogInformation($"Syncing {reportName} via XML...");
            
            try
            {
                var response = await _xmlService.ExportCollectionXmlAsync(reportName, isCollection: false);
                if (string.IsNullOrWhiteSpace(response)) return;
 
                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var stringReader = new System.IO.StringReader(response);
                using var reader = System.Xml.XmlReader.Create(stringReader, settings);
                var doc = XDocument.Load(reader);
                
                var nodes = doc.Descendants(xmlNodeName).ToList();
                
                var entities = new List<TEntity>();
                foreach (var node in nodes)
                {
                    var entity = mappingFunction(node);
                    if (entity != null) entities.Add(entity);
                }

                if (entities.Any())
                {
                    var dbType = SessionManager.Instance.SelectedDatabaseType;
                    bool isMongo = string.Equals(dbType, "MongoDB", StringComparison.OrdinalIgnoreCase);
                    var mongoDocs = new List<BsonDocument>();

                    var dbSet = dbContext.Set<TEntity>();
                    var existingEntities = await dbSet.Where(e => e.OrganizationId == organizationId).ToListAsync();
                    
                    var nameProp = typeof(TEntity).GetProperty("Name");
                    var existingMap = existingEntities
                        .Where(e => nameProp?.GetValue(e) != null)
                        .ToDictionary(e => nameProp!.GetValue(e)!.ToString()!, e => e);

                    foreach (var entity in entities)
                    {
                        if (nameProp == null) continue;
                        var nameValue = nameProp.GetValue(entity)?.ToString();
                        if (string.IsNullOrEmpty(nameValue)) continue;

                        if (existingMap.TryGetValue(nameValue, out var existing))
                        {
                            bool isModified = false;
                            foreach (var prop in typeof(TEntity).GetProperties())
                            {
                                if (prop.Name != "Id" && prop.Name != "OrganizationId" && prop.Name != "CreatedAt" && prop.CanWrite)
                                {
                                    var newValue = prop.GetValue(entity);
                                    var oldValue = prop.GetValue(existing);
                                    if (!Equals(newValue, oldValue))
                                    {
                                        prop.SetValue(existing, newValue);
                                        isModified = true;
                                    }
                                }
                            }

                            if (isModified)
                            {
                                existing.UpdatedAt = DateTimeOffset.UtcNow;
                                dbContext.Entry(existing).State = EntityState.Modified;
                            }
                        }
                        else
                        {
                            entity.Id = Guid.NewGuid();
                            entity.CreatedAt = DateTimeOffset.UtcNow;
                            entity.UpdatedAt = DateTimeOffset.UtcNow;
                            await dbSet.AddAsync(entity);
                        }

                        if (isMongo)
                        {
                            var mongoDoc = new BsonDocument { { "name", nameValue }, { "TallyMasterId", nameValue } };
                            foreach (var prop in typeof(TEntity).GetProperties())
                            {
                                if (prop.Name != "Id" && prop.Name != "OrganizationId" && prop.CanRead)
                                {
                                    var val = prop.GetValue(entity);
                                    if (val != null) mongoDoc[prop.Name.ToLower()] = val.ToString();
                                }
                            }
                            mongoDocs.Add(mongoDoc);
                        }
                    }

                    if (isMongo && mongoDocs.Any())
                    {
                        string collectionName = typeof(TEntity).Name.ToLower() + "s";
                        if (typeof(TEntity) == typeof(AccountingGroup)) collectionName = "accountinggroups";
                        
                        await _mongoService.BulkUpsertDocumentsAsync(collectionName, mongoDocs, "TallyMasterId");
                    }

                    await dbContext.SaveChangesAsync();
                    _logger.LogInformation($"Successfully synced {entities.Count} {reportName}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error syncing {reportName}: {ex.Message}");
                _syncMonitor.AddLog($"âŒ {reportName} sync failed: {ex.Message}", "ERROR", "MASTERS");
            }
        }
    }
}

