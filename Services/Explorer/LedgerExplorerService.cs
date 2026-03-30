using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Acczite20.Services;

namespace Acczite20.Services.Explorer
{
    public class LedgerListItem
    {
        public Guid Id { get; set; }
        public string RawId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string ParentGroup { get; set; } = string.Empty;
        public decimal OpeningBalance { get; set; }
        public decimal TotalDebit { get; set; }
        public decimal TotalCredit { get; set; }
        public decimal ClosingBalance { get; set; }
    }

    public class LedgerEntryDto
    {
        public DateTimeOffset Date { get; set; }
        public string VoucherNumber { get; set; } = string.Empty;
        public string Narration { get; set; } = string.Empty;
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
    }

    public class LedgerDetailDto
    {
        public string Name { get; set; } = string.Empty;
        public string ParentGroup { get; set; } = string.Empty;
        public decimal OpeningBalance { get; set; }
        public decimal ClosingBalance { get; set; }
        public List<LedgerEntryDto> Entries { get; set; } = new();
    }

    public class LedgerExplorerService
    {
        private readonly Services.MongoService _mongoService;

        public LedgerExplorerService(Services.MongoService mongoService)
        {
            _mongoService = mongoService;
        }

        public async Task<(List<LedgerListItem> Items, long Total)> SearchLedgersAsync(
            Guid orgId, 
            string search,
            int skip = 0,
            int limit = 50)
        {
            var coll = await _mongoService.GetCollectionAsync("ledgers");
            if (coll == null) return (new List<LedgerListItem>(), 0);

            var builder = MongoDB.Driver.Builders<MongoDB.Bson.BsonDocument>.Filter;
            var filter = _mongoService.GetOrganizationFilter();

            if (!string.IsNullOrWhiteSpace(search))
            {
                var searchFilter = builder.Regex("name", new MongoDB.Bson.BsonRegularExpression(search, "i"));
                filter &= searchFilter;
            }

            var total = await coll.CountDocumentsAsync(filter);
            var accounts = await coll.Find(filter)
                .SortBy(x => x["name"])
                .Skip(skip)
                .Limit(limit)
                .ToListAsync();

            var results = new List<LedgerListItem>();

            foreach (var acc in accounts)
            {
                decimal opBal = 0;
                if (acc.Contains("openingBalance")) opBal = acc["openingBalance"].ToDecimal();
                
                decimal curBal = opBal;
                if (acc.Contains("closingBalance")) curBal = acc["closingBalance"].ToDecimal();

                results.Add(new LedgerListItem
                {
                    Id = Guid.NewGuid(),
                    RawId = acc.GetValue("_id").ToString(),
                    Name = acc.GetValue("name", "—").AsString,
                    ParentGroup = acc.Contains("parentGroup") && acc["parentGroup"].IsString ? acc["parentGroup"].AsString : "—",
                    OpeningBalance = opBal,
                    TotalDebit = 0,
                    TotalCredit = 0,
                    ClosingBalance = curBal
                });
            }

            return (results, total);
        }

        public async Task<LedgerDetailDto?> GetLedgerDetailsAsync(string rawId, Guid orgId)
        {
            if (string.IsNullOrEmpty(rawId) || !ObjectId.TryParse(rawId, out var objectId)) return null;

            var accounts = await _mongoService.GetCollectionAsync("ledgers");
            var journals = await _mongoService.GetCollectionAsync("vouchers");
            if (accounts == null || journals == null) return null;

            var accountFilter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("_id", objectId),
                _mongoService.GetOrganizationFilter());

            var acc = await accounts.Find(accountFilter).FirstOrDefaultAsync();
            if (acc == null) return null;

            var name = acc.GetValue("name").AsString;
            var details = new LedgerDetailDto
            {
                Name = name,
                ParentGroup = acc.Contains("parentGroup") ? acc["parentGroup"].AsString : "—",
                OpeningBalance = acc.Contains("openingBalance") ? acc["openingBalance"].ToDecimal() : 0,
                ClosingBalance = acc.Contains("closingBalance") ? acc["closingBalance"].ToDecimal() : 0
            };

            // Fetch journal entries involving this ledger
            var filter = Builders<BsonDocument>.Filter.And(
                _mongoService.GetOrganizationFilter(),
                Builders<BsonDocument>.Filter.ElemMatch("allLedgerEntries", Builders<BsonDocument>.Filter.Eq("ledgerName", name))
            );

            var relatedJournals = await journals.Find(filter).SortByDescending(j => j["date"]).Limit(100).ToListAsync();

            foreach (var j in relatedJournals)
            {
                var entry = j["allLedgerEntries"].AsBsonArray
                    .FirstOrDefault(le => le["ledgerName"].AsString == name);

                if (entry != null)
                {
                    details.Entries.Add(new LedgerEntryDto
                    {
                        Date = j.GetValue("date").ToUniversalTime(),
                        VoucherNumber = j.GetValue("voucherNumber", j.GetValue("voucherNo", "—")).AsString,
                        Narration = j.GetValue("narration", "").AsString,
                        Debit = entry.AsBsonDocument.Contains("amount") && entry["amount"].ToDecimal() < 0 ? -entry["amount"].ToDecimal() : 0,
                        Credit = entry.AsBsonDocument.Contains("credit") ? entry["credit"].ToDecimal() : 0
                    });
                }
            }

            return details;
        }

        [Obsolete("Use string rawId version")]
        public async Task<LedgerDetailDto?> GetLedgerDetailsAsync(Guid ledgerId, Guid orgId) => null;
    }
}
