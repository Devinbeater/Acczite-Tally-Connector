using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Acczite20.Models.History;

namespace Acczite20.Services.History
{
    public interface ITimelineService
    {
        Task<List<UnifiedActivityLog>> GetRecentActivitiesAsync(int limit = 100);
        Task<List<UnifiedActivityLog>> GetActivitiesByEntityAsync(string entityType, string entityId);
    }

    public class TimelineService : ITimelineService
    {
        private readonly Data.AppDbContext _db;

        public TimelineService(Data.AppDbContext db)
        {
            _db = db;
        }

        public async Task<List<UnifiedActivityLog>> GetRecentActivitiesAsync(int limit = 100)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            return await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync(
                System.Linq.Queryable.OrderByDescending(_db.UnifiedActivityLogs, l => l.Timestamp)
                .Where(l => l.OrganizationId == orgId)
                .Take(limit)
            );
        }

        public async Task<List<UnifiedActivityLog>> GetActivitiesByEntityAsync(string entityType, string entityId)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            return await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync(
                System.Linq.Queryable.OrderByDescending(_db.UnifiedActivityLogs, l => l.Timestamp)
                .Where(l => l.OrganizationId == orgId && l.EntityType == entityType && l.EntityId == entityId)
            );
        }
    }
}
