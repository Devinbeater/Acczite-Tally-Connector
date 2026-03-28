using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Acczite20.Models;

namespace Acczite20.Services.Sync
{
    public interface IMasterRepository
    {
        Task UpsertGroupsAsync(Guid orgId, IEnumerable<AccountingGroup> groups, CancellationToken ct);
        Task UpsertLedgersAsync(Guid orgId, IEnumerable<Ledger> ledgers, CancellationToken ct);
        Task UpsertVoucherTypesAsync(Guid orgId, IEnumerable<VoucherType> types, CancellationToken ct);
        Task UpsertUnitsAsync(Guid orgId, IEnumerable<Unit> units, CancellationToken ct);
        Task UpsertGodownsAsync(Guid orgId, IEnumerable<Godown> godowns, CancellationToken ct);
        Task UpsertStockGroupsAsync(Guid orgId, IEnumerable<StockGroup> groups, CancellationToken ct);
        Task UpsertStockItemsAsync(Guid orgId, IEnumerable<StockItem> items, CancellationToken ct);
        Task UpsertCostCategoriesAsync(Guid orgId, IEnumerable<CostCategory> categories, CancellationToken ct);
        Task UpsertCostCentresAsync(Guid orgId, IEnumerable<CostCentre> centres, CancellationToken ct);
        
        // Metadata access
        Task<HashSet<string>> GetAccountingGroupNamesAsync(Guid orgId);
        Task<HashSet<string>> GetUnitNamesAsync(Guid orgId);
        Task<HashSet<string>> GetStockGroupNamesAsync(Guid orgId);
    }

    public interface ISyncMetadataService
    {
        Task<bool> IsSameHashAsync(Guid orgId, string entityType, string hash);
        Task SaveHashAsync(Guid orgId, string entityType, string hash);
        Task UpdateSyncStatusAsync(Guid orgId, string entityType, bool success, string? error = null);
    }
}
