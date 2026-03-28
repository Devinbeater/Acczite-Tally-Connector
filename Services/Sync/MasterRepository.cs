using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;
using MongoDB.Bson;

namespace Acczite20.Services.Sync
{
    public class MasterRepository : IMasterRepository
    {
        private readonly AppDbContext _context;
        private readonly IMongoProjector _projector;

        public MasterRepository(AppDbContext context, IMongoProjector projector)
        {
            _context = context;
            _projector = projector;
        }

        public async Task UpsertGroupsAsync(Guid orgId, IEnumerable<AccountingGroup> groups, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, groups, _context.AccountingGroups, "accountinggroups", ct);
        }

        public async Task UpsertLedgersAsync(Guid orgId, IEnumerable<Ledger> ledgers, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, ledgers, _context.Ledgers, "ledgers", ct);
        }

        public async Task UpsertVoucherTypesAsync(Guid orgId, IEnumerable<VoucherType> types, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, types, _context.VoucherTypes, "vouchertypes", ct);
        }

        public async Task UpsertUnitsAsync(Guid orgId, IEnumerable<Unit> units, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, units, _context.Units, "units", ct);
        }

        public async Task UpsertGodownsAsync(Guid orgId, IEnumerable<Godown> godowns, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, godowns, _context.Godowns, "godowns", ct);
        }

        public async Task UpsertStockGroupsAsync(Guid orgId, IEnumerable<StockGroup> groups, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, groups, _context.StockGroups, "stockgroups", ct);
        }

        public async Task UpsertStockItemsAsync(Guid orgId, IEnumerable<StockItem> items, CancellationToken ct)
        {
            await UpsertInternalAsync(orgId, items, _context.StockItems, "stockitems", ct);
        }

        // --- Metadata Helpers ---
        public async Task<HashSet<string>> GetAccountingGroupNamesAsync(Guid orgId)
        {
            var names = await _context.AccountingGroups
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .Select(g => g.Name)
                .ToListAsync();
            return new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        }

        public async Task<HashSet<string>> GetUnitNamesAsync(Guid orgId)
        {
            var names = await _context.Units
                .Where(u => u.OrganizationId == orgId && !u.IsDeleted)
                .Select(u => u.Name)
                .ToListAsync();
            return new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        }

        public async Task<HashSet<string>> GetStockGroupNamesAsync(Guid orgId)
        {
            var names = await _context.StockGroups
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted)
                .Select(g => g.Name)
                .ToListAsync();
            return new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        }

        // --- Core Upsert Logic ---
        private async Task UpsertInternalAsync<T>(
            Guid orgId, 
            IEnumerable<T> entities, 
            DbSet<T> dbSet, 
            string mongoCollection,
            CancellationToken ct) where T : BaseEntity
        {
            var list = entities.ToList();
            if (!list.Any()) return;

            var existingEntities = await dbSet
                .Where(x => x.OrganizationId == orgId)
                .ToListAsync(ct);

            var existingByTallyId = existingEntities
                .Select(entity => new { Entity = entity, TallyId = GetTallyId(entity) })
                .Where(x => !string.IsNullOrWhiteSpace(x.TallyId))
                .GroupBy(x => x.TallyId!, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(g => g.Key, g => g.First().Entity, StringComparer.OrdinalIgnoreCase);

            // 1. SQL Upsert
            foreach (var entity in list)
            {
                entity.OrganizationId = orgId;
                entity.UpdatedAt = DateTimeOffset.UtcNow;
                
                // Assuming TallyMasterId is available on the entity (or Name as fallback)
                string? tallyId = GetTallyId(entity);
                T? existing = null;

                if (!string.IsNullOrWhiteSpace(tallyId))
                {
                    existingByTallyId.TryGetValue(tallyId, out existing);
                }

                if (existing != null)
                {
                    _context.Entry(existing).CurrentValues.SetValues(entity);
                    existing.UpdatedAt = DateTimeOffset.UtcNow;
                }
                else
                {
                    entity.Id = Guid.NewGuid();
                    entity.CreatedAt = DateTimeOffset.UtcNow;
                    dbSet.Add(entity);

                    if (!string.IsNullOrWhiteSpace(tallyId))
                    {
                        existingByTallyId[tallyId] = entity;
                    }
                }
            }

            await _context.SaveChangesAsync(ct);

            // 2. Decoupled Mongo Projection
            foreach (var entity in list)
            {
                var bson = ToBsonDocument(entity);
                _projector.Project(mongoCollection, bson);
            }
        }

        private string? GetTallyId(object entity)
        {
            var prop = entity.GetType().GetProperty("TallyMasterId") ?? entity.GetType().GetProperty("Name");
            return prop?.GetValue(entity)?.ToString();
        }

        private BsonDocument ToBsonDocument(object entity)
        {
            var doc = new BsonDocument();
            foreach (var prop in entity.GetType().GetProperty("TallyMasterId") != null ? entity.GetType().GetProperties() : entity.GetType().GetProperties())
            {
                var val = prop.GetValue(entity);
                if (val != null)
                {
                    if (val is Guid g) doc[prop.Name] = g.ToString();
                    else if (val is DateTimeOffset dto) doc[prop.Name] = dto.ToString("O");
                    else doc[prop.Name] = val.ToString();
                }
            }
            return doc;
        }

        // Specific overrides for CostCategory and CostCentre due to type mismatch in previous placeholder
        public async Task UpsertCostCategoriesAsync(Guid orgId, IEnumerable<CostCategory> categories, CancellationToken ct) => 
            await UpsertInternalAsync(orgId, categories, _context.CostCategories, "costcategories", ct);

        public async Task UpsertCostCentresAsync(Guid orgId, IEnumerable<CostCentre> centres, CancellationToken ct) => 
            await UpsertInternalAsync(orgId, centres, _context.CostCentres, "costcentres", ct);
    }
}
