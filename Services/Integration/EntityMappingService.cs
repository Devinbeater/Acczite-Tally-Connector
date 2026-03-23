using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Integration;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Integration
{
    public interface IEntityMappingService
    {
        Task UpdateMappingAsync(Guid orgId, string? entityType, string? mernId, string? tallyId, Guid localId);
        Task<MernMapping?> GetByMernIdAsync(Guid orgId, string? entityType, string? mernId);
        Task<MernMapping?> GetByTallyIdAsync(Guid orgId, string? entityType, string? tallyId);
        Task<MernMapping?> GetByLocalIdAsync(Guid orgId, Guid localId);
        
        // High-level cross-link helpers
        Task LinkProductAsync(Guid orgId, string? mernId, string? tallyStockItemName);
        Task LinkEmployeeAsync(Guid orgId, string? mernId, string? tallyEmployeeName);

        // Pending Mapping Management
        Task<List<PendingMapping>> GetPendingMappingsAsync(Guid orgId);
        Task ApproveMappingAsync(Guid mappingId);
        Task RejectMappingAsync(Guid mappingId);
    }

    public class EntityMappingService : IEntityMappingService
    {
        private readonly AppDbContext _context;

        public EntityMappingService(AppDbContext context)
        {
            _context = context;
        }

        public async Task UpdateMappingAsync(Guid orgId, string? entityType, string? mernId, string? tallyId, Guid localId)
        {
            var mapping = await _context.MernMappings
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType && (m.MernId == mernId || m.TallyMasterId == tallyId));

            if (mapping == null)
            {
                mapping = new MernMapping
                {
                    OrganizationId = orgId,
                    EntityType = entityType,
                    MernId = mernId,
                    TallyMasterId = tallyId,
                    LocalId = localId,
                    CreatedAt = DateTimeOffset.UtcNow
                };
                _context.MernMappings.Add(mapping);
            }
            else
            {
                mapping.MernId = mernId;
                mapping.TallyMasterId = tallyId;
                mapping.LocalId = localId;
                mapping.UpdatedAt = DateTimeOffset.UtcNow;
                mapping.LastSyncAt = DateTimeOffset.UtcNow;
            }

            await _context.SaveChangesAsync();
        }

        public async Task<MernMapping?> GetByMernIdAsync(Guid orgId, string? entityType, string? mernId)
        {
            return await _context.MernMappings
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType && m.MernId == mernId);
        }

        public async Task<MernMapping?> GetByTallyIdAsync(Guid orgId, string? entityType, string? tallyId)
        {
            return await _context.MernMappings
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType && m.TallyMasterId == tallyId);
        }

        public async Task<MernMapping?> GetByLocalIdAsync(Guid orgId, Guid localId)
        {
            return await _context.MernMappings
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.LocalId == localId);
        }

        public async Task LinkProductAsync(Guid orgId, string? mernId, string? tallyStockItemName)
        {
            // Find local DimStockItem by Name (Sync from Tally should have populated this)
            var stockItem = await _context.DimStockItems
                .FirstOrDefaultAsync(s => s.OrganizationId == orgId && s.StockItemName == tallyStockItemName);

            if (stockItem != null)
            {
                await UpdateMappingAsync(orgId, "Product", mernId, stockItem.TallyMasterId ?? tallyStockItemName, stockItem.Id);
            }
        }

        public async Task LinkEmployeeAsync(Guid orgId, string? mernId, string? tallyEmployeeName)
        {
            // For employees, we might link to a specific Ledger if payroll is handled via ledger entries
            // or to a Payroll entity.
            var mapping = await _context.MernMappings
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == "Employee" && m.MernId == mernId);

            if (mapping == null)
            {
                _context.MernMappings.Add(new MernMapping
                {
                    OrganizationId = orgId,
                    EntityType = "Employee",
                    MernId = mernId,
                    TallyMasterId = tallyEmployeeName,
                    LocalId = Guid.NewGuid() // Link to synthetic local ID if no direct SQL entry yet
                });
                await _context.SaveChangesAsync();
            }
        }
        public async Task<List<PendingMapping>> GetPendingMappingsAsync(Guid orgId)
        {
            return await _context.PendingMappings
                .Where(m => m.OrganizationId == orgId && m.Status == "Pending")
                .OrderByDescending(m => m.ConfidenceScore)
                .ToListAsync();
        }

        public async Task ApproveMappingAsync(Guid mappingId)
        {
            var pending = await _context.PendingMappings.FindAsync(mappingId);
            if (pending == null) return;

            // 1. Create or update the actual mapping
            if (pending.EntityType == "Product")
            {
                await LinkProductAsync(pending.OrganizationId, pending.MernId, pending.SuggestedTallyName);
            }
            else if (pending.EntityType == "Employee")
            {
                await LinkEmployeeAsync(pending.OrganizationId, pending.MernId, pending.SuggestedTallyName);
            }

            // 2. Mark as confirmed
            pending.Status = "Confirmed";
            await _context.SaveChangesAsync();

            // 3. Notify System
            await Acczite20.Core.Events.EventBus.PublishAsync(new Acczite20.Core.Events.MappingApprovedEvent(
                pending.EntityType, 
                pending.MernDisplayName, 
                pending.SuggestedTallyName));
        }

        public async Task RejectMappingAsync(Guid mappingId)
        {
            var pending = await _context.PendingMappings.FindAsync(mappingId);
            if (pending != null)
            {
                pending.Status = "Rejected";
                await _context.SaveChangesAsync();
            }
        }
    }
}
