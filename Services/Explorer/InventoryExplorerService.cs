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

        public async Task<List<StockItemDto>> SearchStockItemsAsync(string search)
        {
            if (string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase))
            {
                return await SearchMongoStockItemsAsync(search);
            }

            var query = _context.StockItems.IgnoreQueryFilters().AsQueryable();

            if (!string.IsNullOrWhiteSpace(search))
            {
                query = query.Where(s => s.Name.Contains(search) || s.StockGroup.Contains(search));
            }

            var items = await query
                .OrderBy(s => s.Name)
                .Take(200)
                .ToListAsync();

            return items.Select(s => new StockItemDto
            {
                Name = s.Name,
                StockGroup = s.StockGroup,
                Unit = s.BaseUnit,
                ClosingBalance = s.ClosingBalance
            }).ToList();
        }

        private async Task<List<StockItemDto>> SearchMongoStockItemsAsync(string search)
        {
            var collection = await _mongoService.GetCollectionAsync("stockitems", "inventory", "stockitem", "products");
            if (collection == null) return new List<StockItemDto>();
            
            var filter = _mongoService.GetOrganizationFilter();

            if (!string.IsNullOrWhiteSpace(search))
            {
                var searchFilter = Builders<BsonDocument>.Filter.Or(
                    Builders<BsonDocument>.Filter.Regex("name", new BsonRegularExpression(search, "i")),
                    Builders<BsonDocument>.Filter.Regex("stockGroup", new BsonRegularExpression(search, "i"))
                );
                filter = Builders<BsonDocument>.Filter.And(filter, searchFilter);
            }

            var items = await collection.Find(filter)
                .SortBy(x => x["name"])
                .Limit(200)
                .ToListAsync();

            return items.Select(i => new StockItemDto
            {
                Name = i.GetValue("name", "").ToString(),
                StockGroup = i.GetValue("stockGroup", "").ToString(),
                Unit = i.GetValue("unit", i.GetValue("baseUnit", "")).ToString(),
                ClosingBalance = i.GetValue("closingBalance", 0).ToDecimal()
            }).ToList();
        }
    }
}
