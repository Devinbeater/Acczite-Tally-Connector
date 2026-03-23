using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using MongoDB.Bson;

namespace Acczite20.Services
{
    public class MongoService
    {
        private static readonly string[] RelevantCollectionNames =
        {
            "accounts",
            "accountgroups",
            "accountinggroups",
            "accountinginvoices",
            "journals",
            "vouchers",
            "expenses",
            "stockitems",
            "stockgroups",
            "stockcategories",
            "inventory",
            "products",
            "admininventories",
            "attendances",
            "employees",
            "payroll",
            "payrolls",
            "vouchertypes",
            "currencies",
            "units",
            "godowns",
            "costcentres",
            "costcenters"
        };

        private static readonly string[] RelevantCollectionPrefixes =
        {
            "account",
            "ledger",
            "journal",
            "voucher",
            "invoice",
            "expense",
            "stock",
            "inventory",
            "product",
            "attendance",
            "employee",
            "payroll",
            "admininventor"
        };

        private readonly IMongoClient _client;
        private readonly SessionManager _session;

        public MongoService(IMongoClient client, SessionManager session)
        {
            _client = client;
            _session = session;
        }

        public async Task<List<string>> ListCollectionsAsync()
        {
            var db = GetDatabase();
            var collections = await db.ListCollectionNamesAsync();
            return await collections.ToListAsync();
        }

        public static List<string> FilterRelevantCollections(IEnumerable<string>? collectionNames)
        {
            if (collectionNames == null)
            {
                return new List<string>();
            }

            return collectionNames
                .Where(IsRelevantCollectionName)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(GetRelevantCollectionSortKey)
                .ThenBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        public static bool IsRelevantCollectionName(string? collectionName)
        {
            if (string.IsNullOrWhiteSpace(collectionName))
            {
                return false;
            }

            return RelevantCollectionNames.Contains(collectionName, StringComparer.OrdinalIgnoreCase)
                || RelevantCollectionPrefixes.Any(prefix => collectionName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
        }

        private static int GetRelevantCollectionSortKey(string collectionName)
        {
            if (collectionName.StartsWith("account", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("ledger", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("journal", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("voucher", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("invoice", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("expense", StringComparison.OrdinalIgnoreCase))
            {
                return 0;
            }

            if (collectionName.StartsWith("stock", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("inventory", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("product", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("admininventor", StringComparison.OrdinalIgnoreCase))
            {
                return 1;
            }

            if (collectionName.StartsWith("attendance", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("employee", StringComparison.OrdinalIgnoreCase)
                || collectionName.StartsWith("payroll", StringComparison.OrdinalIgnoreCase))
            {
                return 2;
            }

            return 3;
        }

        public async Task<IMongoCollection<BsonDocument>?> GetCollectionAsync(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var db = GetDatabase();
            // We return the collection even if it doesn't exist yet in the metadata.
            // MongoDB will create it automatically on the first write operation.
            return db.GetCollection<BsonDocument>(name);
        }

        public async Task<IMongoCollection<BsonDocument>?> GetCollectionAsync(params string[] names)
        {
            if (names == null || names.Length == 0)
            {
                return null;
            }

            var db = GetDatabase();
            var collections = await ListCollectionsAsync();
            
            // 1. Try to find an existing one
            var targetName = names.FirstOrDefault(name =>
                !string.IsNullOrWhiteSpace(name) &&
                collections.Any(c => c.Equals(name, StringComparison.OrdinalIgnoreCase)));

            if (targetName != null)
            {
                return db.GetCollection<BsonDocument>(targetName);
            }

            // 2. If none exist, use the first preferred name (Mongo will create it on write)
            return db.GetCollection<BsonDocument>(names[0]);
        }

        public FilterDefinition<BsonDocument> GetOrganizationFilter()
        {
            return GetOrganizationFilter("organizationId", "orgId", "companyId", "OrganizationId");
        }

        public FilterDefinition<BsonDocument> GetOrganizationFilter(params string[] fieldNames)
        {
            var builder = Builders<BsonDocument>.Filter;
            var fields = (fieldNames == null || fieldNames.Length == 0)
                ? new[] { "organizationId" }
                : fieldNames.Where(name => !string.IsNullOrWhiteSpace(name)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

            var filters = new List<FilterDefinition<BsonDocument>>();

            // Try Mongo ObjectId
            if (!string.IsNullOrEmpty(_session.OrganizationObjectId) && ObjectId.TryParse(_session.OrganizationObjectId, out var orgObjectId))
            {
                foreach (var field in fields)
                {
                    filters.Add(builder.Eq(field, orgObjectId));
                    filters.Add(builder.Eq(field, _session.OrganizationObjectId)); // Also try as string
                }
            }
            else if (!string.IsNullOrEmpty(_session.OrganizationObjectId))
            {
                foreach (var field in fields)
                {
                    filters.Add(builder.Eq(field, _session.OrganizationObjectId));
                }
            }

            // Try SQL Guid
            if (_session.OrganizationId != Guid.Empty)
            {
                var orgIdString = _session.OrganizationId.ToString();
                foreach (var field in fields)
                {
                    filters.Add(builder.Eq(field, orgIdString));
                    if (Guid.TryParse(orgIdString, out var guid))
                    {
                         // Some Mongo drivers might store it as Guid/Binary
                         filters.Add(builder.Eq(field, guid));
                    }
                }
            }

            if (filters.Count == 0)
            {
                return builder.Empty; // Fallback to all if no org context (caution)
            }

            return filters.Count == 1 ? filters[0] : builder.Or(filters);
        }

        public async Task<bool> UpsertDocumentAsync(string collectionName, BsonDocument document, string filterField = "TallyMasterId")
        {
            var collection = await GetCollectionAsync(collectionName);
            if (collection == null) return false;
            
            var idValue = document.GetValue(filterField, null);
            if (idValue == null || idValue.IsBsonNull) return false;

            var filter = Builders<BsonDocument>.Filter.Eq(filterField, idValue);
            
            document["organizationId"] = GetOrganizationIdValue();
            document["lastModified"] = DateTimeOffset.UtcNow.ToString("O");

            await collection.ReplaceOneAsync(filter, document, new ReplaceOptions { IsUpsert = true });
            return true;
        }

        public async Task<int> BulkUpsertDocumentsAsync(string collectionName, IEnumerable<BsonDocument> documents, string filterField = "TallyMasterId")
        {
            var collection = await GetCollectionAsync(collectionName);
            if (collection == null) return 0;

            var models = new List<WriteModel<BsonDocument>>();
            var orgIdValue = GetOrganizationIdValue();
            var now = DateTimeOffset.UtcNow.ToString("O");

            foreach (var doc in documents)
            {
                var idValue = doc.GetValue(filterField, null);
                if (idValue == null || idValue.IsBsonNull) continue;

                doc["organizationId"] = orgIdValue;
                doc["lastModified"] = now;

                var filter = Builders<BsonDocument>.Filter.Eq(filterField, idValue);
                models.Add(new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true });
            }

            if (models.Count == 0) return 0;

            var result = await collection.BulkWriteAsync(models);
            return (int)(result.Upserts.Count + result.ModifiedCount);
        }

        public async Task<int> BulkDeleteDocumentsAsync(string collectionName, string filterField, IEnumerable<string> ids)
        {
            var collection = await GetCollectionAsync(collectionName);
            if (collection == null || ids == null || !ids.Any()) return 0;
            
            var filter = Builders<BsonDocument>.Filter.In(filterField, ids);
            var result = await collection.DeleteManyAsync(filter);
            return (int)result.DeletedCount;
        }

        private BsonValue GetOrganizationIdValue()
        {
            if (!string.IsNullOrEmpty(_session.OrganizationObjectId) && ObjectId.TryParse(_session.OrganizationObjectId, out var orgObjectId))
                return orgObjectId;
            
            return _session.OrganizationId != Guid.Empty ? _session.OrganizationId.ToString() : BsonNull.Value;
        }

        public IMongoDatabase GetDatabase()
        {
            if (!string.IsNullOrEmpty(_session.DatabaseName))
                return _client.GetDatabase(_session.DatabaseName);

            if (!string.IsNullOrEmpty(_session.ConnectionString))
            {
                var url = new MongoUrl(_session.ConnectionString);
                if (!string.IsNullOrEmpty(url.DatabaseName))
                    return _client.GetDatabase(url.DatabaseName);
            }

            try
            {
                if (System.IO.File.Exists("dbconfig.json"))
                {
                    var json = System.IO.File.ReadAllText("dbconfig.json");
                    var doc = System.Text.Json.JsonDocument.Parse(json);
                    if (doc.RootElement.TryGetProperty("MongoUri", out var uriProp))
                    {
                        var connUrl = new MongoUrl(uriProp.GetString());
                        if (!string.IsNullOrEmpty(connUrl.DatabaseName))
                            return _client.GetDatabase(connUrl.DatabaseName);
                    }
                }
            }
            catch { }

            return _client.GetDatabase("acczite_master");
        }
    }
}
