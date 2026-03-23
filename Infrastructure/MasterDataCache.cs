using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Infrastructure
{
    public class MasterDataCache
    {
        private readonly AppDbContext _dbContext;
        private Dictionary<string, Guid> _ledgerIds = new Dictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, Guid> _stockItemIds = new Dictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, Guid> _voucherTypeIds = new Dictionary<string, Guid>(StringComparer.OrdinalIgnoreCase);

        public MasterDataCache(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task InitializeAsync(Guid organizationId)
        {
            // Load IDs for fast lookup during bulk sync
            _ledgerIds = await _dbContext.Ledgers
                .Where(l => l.OrganizationId == organizationId)
                .ToDictionaryAsync(l => l.Name, l => l.Id, StringComparer.OrdinalIgnoreCase);

            _stockItemIds = await _dbContext.StockItems
                .Where(s => s.OrganizationId == organizationId)
                .ToDictionaryAsync(s => s.Name, s => s.Id, StringComparer.OrdinalIgnoreCase);

            _voucherTypeIds = await _dbContext.VoucherTypes
                .Where(v => v.OrganizationId == organizationId)
                .ToDictionaryAsync(v => v.Name, v => v.Id, StringComparer.OrdinalIgnoreCase);
        }

        public Guid GetLedgerId(string name) => _ledgerIds.TryGetValue(name, out var id) ? id : Guid.Empty;
        public Guid GetStockItemId(string name) => _stockItemIds.TryGetValue(name, out var id) ? id : Guid.Empty;
        public Guid GetVoucherTypeId(string name) => _voucherTypeIds.TryGetValue(name, out var id) ? id : Guid.Empty;
    }
}
