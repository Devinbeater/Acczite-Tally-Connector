using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Acczite20.Data;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Acczite20.Services.Explorer
{
    public class StockItemDto
    {
        public string Name { get; set; } = string.Empty;
        public string StockGroup { get; set; } = string.Empty;
        public string Unit { get; set; } = string.Empty;
        public decimal ClosingBalance { get; set; }
    }

    public class InventoryExplorerService
    {
        private readonly AppDbContext _context;
        private readonly MongoService _mongoService;

        public InventoryExplorerService(AppDbContext context, MongoService mongoService)
        {
            _context = context;
            _mongoService = mongoService;
        }

        public async Task<(List<StockItemDto> Items, long Total)> SearchStockItemsAsync(string search, int skip = 0, int limit = 50)
        {
            if (string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase))
            {
                return await SearchMongoStockItemsAsync(search, skip, limit);
            }

            var query = _context.StockItems.IgnoreQueryFilters().AsQueryable();

            if (!string.IsNullOrWhiteSpace(search))
            {
                query = query.Where(s => s.Name.Contains(search) || s.StockGroup.Contains(search));
            }

            var total = await query.CountAsync();
            var items = await query
                .OrderBy(s => s.Name)
                .Skip(skip)
                .Take(limit)
                .ToListAsync();

            var results = items.Select(s => new StockItemDto
            {
                Name = s.Name,
                StockGroup = s.StockGroup,
                Unit = s.BaseUnit,
                ClosingBalance = s.ClosingBalance
            }).ToList();

            return (results, (long)total);
        }

        private async Task<(List<StockItemDto> Items, long Total)> SearchMongoStockItemsAsync(string search, int skip = 0, int limit = 50)
        {
            var collection = await _mongoService.GetCollectionAsync("stockitems", "inventory", "stockitem", "products");
            if (collection == null) return (new List<StockItemDto>(), 0);
            
            var filter = _mongoService.GetOrganizationFilter();

            if (!string.IsNullOrWhiteSpace(search))
            {
                var searchFilter = Builders<BsonDocument>.Filter.Or(
                    Builders<BsonDocument>.Filter.Regex("name", new BsonRegularExpression(search, "i")),
                    Builders<BsonDocument>.Filter.Regex("stockGroup", new BsonRegularExpression(search, "i"))
                );
                filter = Builders<BsonDocument>.Filter.And(filter, searchFilter);
            }

            var total = await collection.CountDocumentsAsync(filter);
            var items = await collection.Find(filter)
                .SortBy(x => x["name"])
                .Skip(skip)
                .Limit(limit)
                .ToListAsync();

            var results = items.Select(i => new StockItemDto
            {
                Name = i.GetValue("name", "").ToString(),
                StockGroup = i.GetValue("stockGroup", "").ToString(),
                Unit = i.GetValue("unit", i.GetValue("baseUnit", "")).ToString(),
                ClosingBalance = i.GetValue("closingBalance", 0).ToDecimal()
            }).ToList();

            return (results, total);
        }
    }
}
