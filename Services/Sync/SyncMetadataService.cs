using System;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Sync
{
    public class SyncMetadataService : ISyncMetadataService
    {
        private readonly AppDbContext _context;

        public SyncMetadataService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<bool> IsSameHashAsync(Guid orgId, string entityType, string hash)
        {
            if (string.IsNullOrEmpty(hash)) return false;
            
            var meta = await _context.SyncMetadataRecords
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType);
            
            return meta != null && meta.EntityHash == hash;
        }

        public async Task SaveHashAsync(Guid orgId, string entityType, string hash)
        {
            var meta = await _context.SyncMetadataRecords
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType);
            
            if (meta == null)
            {
                meta = new SyncMetadata
                {
                    OrganizationId = orgId,
                    CompanyId = Guid.Empty, // Will be updated if company context is refined
                    EntityType = entityType,
                    CreatedAt = DateTimeOffset.UtcNow
                };
                _context.SyncMetadataRecords.Add(meta);
            }
            
            meta.EntityHash = hash;
            meta.LastSuccessfulSync = DateTimeOffset.UtcNow;
            meta.UpdatedAt = DateTimeOffset.UtcNow;
            
            await _context.SaveChangesAsync();
        }

        public async Task UpdateSyncStatusAsync(Guid orgId, string entityType, bool success, string? error = null)
        {
            var meta = await _context.SyncMetadataRecords
                .FirstOrDefaultAsync(m => m.OrganizationId == orgId && m.EntityType == entityType);
            
            if (meta == null)
            {
                meta = new SyncMetadata
                {
                    OrganizationId = orgId,
                    EntityType = entityType
                };
                _context.SyncMetadataRecords.Add(meta);
            }
            
            meta.LastError = success ? null : error;
            meta.LastSuccessfulSync = success ? DateTimeOffset.UtcNow : meta.LastSuccessfulSync;
            meta.UpdatedAt = DateTimeOffset.UtcNow;
            
            await _context.SaveChangesAsync();
        }
    }
}
