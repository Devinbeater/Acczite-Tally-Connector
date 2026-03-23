using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Acczite20.Services;

namespace Acczite20.Services.Explorer
{
    public class VoucherListItem
    {
        public Guid Id { get; set; }
        public string RawId { get; set; } = string.Empty;
        public string VoucherNumber { get; set; } = string.Empty;
        public DateTimeOffset Date { get; set; }
        public string Narration { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    public class VoucherDetailDto
    {
        public string VoucherNumber { get; set; } = string.Empty;
        public string Narration { get; set; } = string.Empty;
        public DateTimeOffset Date { get; set; }
        public decimal TotalAmount { get; set; }
        public List<BsonDocument> LedgerEntries { get; set; } = new();
        public List<BsonDocument> Inventory { get; set; } = new();
        public List<BsonDocument> Gst { get; set; } = new();
    }

    public class VoucherExplorerService
    {
        private readonly Services.MongoService _mongoService;

        public VoucherExplorerService(Services.MongoService mongoService)
        {
            _mongoService = mongoService;
        }

        public async Task<List<VoucherListItem>> SearchVouchersAsync(
            Guid orgId,
            string search,
            DateTime? from,
            DateTime? to)
        {
            var coll = await _mongoService.GetCollectionAsync("journals", "vouchers");
            if (coll == null) return new List<VoucherListItem>();

            var builder = MongoDB.Driver.Builders<MongoDB.Bson.BsonDocument>.Filter;
            var filter = _mongoService.GetOrganizationFilter();

            if (!string.IsNullOrEmpty(search))
            {
                filter &= builder.Regex("voucherNumber", new MongoDB.Bson.BsonRegularExpression(search, "i"));
            }

            if (from.HasValue)
            {
                filter &= builder.Gte("date", from.Value.ToUniversalTime());
            }

            if (to.HasValue)
            {
                filter &= builder.Lte("date", to.Value.ToUniversalTime());
            }

            var results = await coll.Find(filter)
                .Sort(MongoDB.Driver.Builders<MongoDB.Bson.BsonDocument>.Sort.Descending("date"))
                .Limit(200)
                .ToListAsync();

            return results.Select(doc => new VoucherListItem
            {
                Id = Guid.NewGuid(), // UI expects Guid
                RawId = doc.GetValue("_id").ToString(),
                VoucherNumber = doc.GetValue("voucherNumber", doc.GetValue("voucherNo", "—")).AsString,
                Date = doc.GetValue("date").ToUniversalTime(),
                Narration = doc.GetValue("narration", "").AsString,
                Amount = doc.GetValue("totalAmount", 0).ToDecimal()
            }).ToList();
        }

        public async Task<VoucherDetailDto?> GetVoucherAsync(string rawId, Guid orgId)
        {
            if (string.IsNullOrEmpty(rawId) || !ObjectId.TryParse(rawId, out var objectId)) return null;

            var coll = await _mongoService.GetCollectionAsync("journals", "vouchers");
            if (coll == null) return null;

            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("_id", objectId),
                _mongoService.GetOrganizationFilter());

            var doc = await coll.Find(filter).FirstOrDefaultAsync();
            if (doc == null) return null;

            var details = new VoucherDetailDto
            {
                VoucherNumber = doc.GetValue("voucherNumber", doc.GetValue("voucherNo", "—")).AsString,
                Narration = doc.GetValue("narration", "").AsString,
                Date = doc.GetValue("date").ToUniversalTime(),
                TotalAmount = doc.GetValue("totalAmount", 0).ToDecimal()
            };

            if (doc.Contains("allLedgerEntries") && doc["allLedgerEntries"].IsBsonArray)
                details.LedgerEntries = doc["allLedgerEntries"].AsBsonArray.Select(x => x.AsBsonDocument).ToList();

            if (doc.Contains("allInventoryEntries") && doc["allInventoryEntries"].IsBsonArray)
                details.Inventory = doc["allInventoryEntries"].AsBsonArray.Select(x => x.AsBsonDocument).ToList();

            if (doc.Contains("gstEntries") && doc["gstEntries"].IsBsonArray)
                details.Gst = doc["gstEntries"].AsBsonArray.Select(x => x.AsBsonDocument).ToList();

            return details;
        }

        [Obsolete("Use string rawId version")]
        public async Task<VoucherDetailDto?> GetVoucherAsync(Guid id, Guid orgId) => null;
    }
}
