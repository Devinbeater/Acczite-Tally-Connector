using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Acczite20.Models;
using Acczite20.Services.Tally;
using Microsoft.Extensions.Logging;

namespace Acczite20.Services.Sync
{
    public class TallyMasterSyncService
    {
        private readonly TallyXmlService _xml;
        private readonly IMasterRepository _repo;
        private readonly ISyncMetadataService _meta;
        private readonly ILogger<TallyMasterSyncService> _log;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly ISyncControlService _syncControl;

        public TallyMasterSyncService(
            TallyXmlService xml,
            IMasterRepository repo,
            ISyncMetadataService meta,
            ILogger<TallyMasterSyncService> log,
            SyncStateMonitor syncMonitor,
            ISyncControlService syncControl)
        {
            _xml = xml;
            _repo = repo;
            _meta = meta;
            _log = log;
            _syncMonitor = syncMonitor;
            _syncControl = syncControl;
        }

        public async Task SyncAllMastersAsync(Guid orgId, bool force = false, CancellationToken ct = default)
        {
            _log.LogInformation("Master Sync Started for {Org}", orgId);
            _syncMonitor.AddLog($"Starting Deterministic Master Sync for Org: {orgId}", "INFO", "MASTERS");

            // ORDER = STRICT DEPENDENCY GRAPH
            await SyncPhase("Groups", orgId, () => SyncGroupsAsync(orgId, force, ct), ct);
            await SyncPhase("Ledgers", orgId, () => SyncLedgersAsync(orgId, force, ct), ct);
            await SyncPhase("VoucherTypes", orgId, () => SyncVoucherTypesAsync(orgId, force, ct), ct);

            await SyncPhase("Units", orgId, () => SyncUnitsAsync(orgId, force, ct), ct);
            await SyncPhase("Godowns", orgId, () => SyncGodownsAsync(orgId, force, ct), ct);

            await SyncPhase("StockGroups", orgId, () => SyncStockGroupsAsync(orgId, force, ct), ct);
            await SyncPhase("StockItems", orgId, () => SyncStockItemsAsync(orgId, force, ct), ct);

            await SyncPhase("CostCategories", orgId, () => SyncCostCategoriesAsync(orgId, force, ct), ct);
            await SyncPhase("CostCentres", orgId, () => SyncCostCentresAsync(orgId, force, ct), ct);

            _log.LogInformation("Master Sync Completed for {Org}", orgId);
            _syncMonitor.AddLog("✅ Master Data Sync Cycle Finished (Deterministic).", "SUCCESS", "MASTERS");
        }

        private async Task SyncPhase(string name, Guid orgId, Func<Task<int>> action, CancellationToken ct)
        {
            try
            {
                var control = _syncControl.GetState(orgId);
                if (control.IsPaused)
                {
                    _syncMonitor.SetStage($"Paused {name}", "Sync is paused. Click Resume to continue.", _syncMonitor.ProgressPercent, false);
                }
                await control.PauseGate.WaitAsync(ct);
                control.PauseGate.Release();

                ct.ThrowIfCancellationRequested();
                control.CurrentPhase = $"Master Sync: {name}";

                _log.LogInformation("Phase Start: {Phase}", name);
                _syncMonitor.SetStage($"Master Sync: {name}", $"Processing {name}...", 0, false);
                
                var sw = System.Diagnostics.Stopwatch.StartNew();
                int count = await action();
                sw.Stop();
                
                if (count > 0)
                {
                    _syncMonitor.TotalRecordsSynced += count;
                    _syncMonitor.UpdateMetrics(_syncMonitor.TotalRecordsSynced, sw.Elapsed.TotalSeconds);
                    _log.LogInformation("Phase Complete: {Phase}. Synced {Count} records.", name, count);
                }
                else
                {
                    _log.LogInformation("Phase Complete: {Phase}. No new data or records found.", name);
                }

                _log.LogInformation("Phase Complete: {Phase}", name);
                _syncMonitor.AddLog($"✅ {name} phase complete. ({count} records)", "SUCCESS", "MASTERS");
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Phase Failed: {Phase}", name);
                _syncMonitor.AddLog($"❌ {name} phase failed: {ex.Message}", "ERROR", "MASTERS");
                throw; // FAIL FAST — do not continue
            }
        }

        // --- Phase 1: Accounting Groups ---
        private async Task<int> SyncGroupsAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetGroupsAsync();
            var normalized = data
                .Select(x => new {
                    Name = x.Name.Trim(),
                    Parent = x.Parent?.Trim(),
                    TallyMasterId = x.TallyMasterId,
                    TallyAlterId = x.TallyAlterId
                })
                .OrderBy(x => x.Parent)
                .ThenBy(x => x.Name)
                .ToList();

            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "Groups", hash))
            {
                _log.LogInformation("Groups unchanged, skipping.");
                return 0;
            }

            var entities = normalized.Select(x => new AccountingGroup
            {
                Name = x.Name,
                Parent = x.Parent,
                TallyMasterId = x.TallyMasterId
            });

            await _repo.UpsertGroupsAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "Groups", hash);
            return normalized.Count;
        }

        // --- Phase 2: Ledgers ---
        private async Task<int> SyncLedgersAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetLedgersAsync();
            var normalized = data
                .Select(x => new {
                    Name = x.Name.Trim(),
                    Parent = x.Parent?.Trim(),
                    TallyMasterId = x.TallyMasterId,
                    TallyAlterId = x.TallyAlterId
                })
                .OrderBy(x => x.Parent)
                .ThenBy(x => x.Name)
                .ToList();

            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "Ledgers", hash)) return 0;

            // VALIDATE dependencies BEFORE insert
            var validGroups = await _repo.GetAccountingGroupNamesAsync(orgId);
            var validEntities = normalized
                .Where(x => string.IsNullOrEmpty(x.Parent) || validGroups.Contains(x.Parent))
                .Select(x => new Ledger { Name = x.Name, ParentGroup = x.Parent, TallyMasterId = x.TallyMasterId })
                .ToList();

            await _repo.UpsertLedgersAsync(orgId, validEntities, ct);
            await _meta.SaveHashAsync(orgId, "Ledgers", hash);
            return normalized.Count;
        }

        // --- Phase 4: Voucher Types ---
        private async Task<int> SyncVoucherTypesAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetVoucherTypesAsync();
            var normalized = data
                .Select(x => new { Name = x.Name.Trim(), Category = x.Parent?.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId })
                .OrderBy(x => x.Name)
                .ToList();

            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "VoucherTypes", hash)) return 0;

            var entities = normalized.Select(x => new VoucherType { Name = x.Name, Category = x.Category ?? string.Empty, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertVoucherTypesAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "VoucherTypes", hash);
            return normalized.Count;
        }

        // --- Phase 5: Units & Godowns ---
        private async Task<int> SyncUnitsAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetUnitsAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "Units", hash)) return 0;

            var entities = normalized.Select(x => new Unit { Name = x.Name, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertUnitsAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "Units", hash);
            return normalized.Count;
        }

        private async Task<int> SyncGodownsAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetGodownsAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), Parent = x.Parent?.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Parent).ThenBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "Godowns", hash)) return 0;

            var entities = normalized.Select(x => new Godown { Name = x.Name, Parent = x.Parent ?? string.Empty, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertGodownsAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "Godowns", hash);
            return normalized.Count;
        }

        // --- Phase 6: Stock Groups & Items ---
        private async Task<int> SyncStockGroupsAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetStockGroupsAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), Parent = x.Parent?.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Parent).ThenBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "StockGroups", hash)) return 0;

            var entities = normalized.Select(x => new StockGroup { Name = x.Name, Parent = x.Parent ?? string.Empty, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertStockGroupsAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "StockGroups", hash);
            return normalized.Count;
        }

        private async Task<int> SyncStockItemsAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetStockItemsAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), Group = x.Parent?.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Group).ThenBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "StockItems", hash)) return 0;

            var validGroups = await _repo.GetStockGroupNamesAsync(orgId);
            var validEntities = normalized
                .Where(x => string.IsNullOrEmpty(x.Group) || validGroups.Contains(x.Group))
                .Select(x => new StockItem { Name = x.Name, StockGroup = x.Group ?? string.Empty, TallyMasterId = x.TallyMasterId ?? string.Empty })
                .ToList();

            await _repo.UpsertStockItemsAsync(orgId, validEntities, ct);
            await _meta.SaveHashAsync(orgId, "StockItems", hash);
            return validEntities.Count;
        }

        // --- Phase 7: Cost Categories & Centres ---
        private async Task<int> SyncCostCategoriesAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetCostCategoriesAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "CostCategories", hash)) return 0;

            var entities = normalized.Select(x => new CostCategory { Name = x.Name, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertCostCategoriesAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "CostCategories", hash);
            return normalized.Count;
        }

        private async Task<int> SyncCostCentresAsync(Guid orgId, bool force, CancellationToken ct)
        {
            var data = await _xml.GetCostCentresAsync();
            var normalized = data.Select(x => new { Name = x.Name.Trim(), Category = x.Parent?.Trim(), TallyMasterId = x.TallyMasterId, TallyAlterId = x.TallyAlterId }).OrderBy(x => x.Category).ThenBy(x => x.Name).ToList();
            var hash = CalculateHash(normalized);
            if (!force && await _meta.IsSameHashAsync(orgId, "CostCentres", hash)) return 0;

            var entities = normalized.Select(x => new CostCentre { Name = x.Name, CategoryName = x.Category, TallyMasterId = x.TallyMasterId });
            await _repo.UpsertCostCentresAsync(orgId, entities, ct);
            await _meta.SaveHashAsync(orgId, "CostCentres", hash);
            return normalized.Count;
        }

        private string CalculateHash(IEnumerable<dynamic> data)
        {
            // Lightweight Hashing: Only hash Identity + Version (AlterId)
            var identityStream = string.Join("|", data.Select(x => $"{x.TallyMasterId}:{x.TallyAlterId}"));
            using var sha = SHA256.Create();
            var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(identityStream));
            return Convert.ToHexString(bytes);
        }
    }
}
