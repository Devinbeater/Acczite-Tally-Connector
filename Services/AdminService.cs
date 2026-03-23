using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Acczite20.MongoDocuments;
using Acczite20.Data;
using MongoDB.Bson;

namespace Acczite20.Services
{
    public class AdminService
    {
        private readonly IMongoClient _client;
        private readonly SessionManager _session;
        private readonly MongoService _mongoService;

        public AdminService(IMongoClient client, SessionManager session, MongoService mongoService)
        {
            _client = client;
            _session = session;
            _mongoService = mongoService;
        }

        private IMongoDatabase GetDatabase() => _mongoService.GetDatabase();

        public async Task<List<AdminProductDocument>> GetProductsAsync()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<AdminProductDocument>("products");

            var filter = Builders<AdminProductDocument>.Filter.Empty;
            if (ObjectId.TryParse(_session.OrganizationObjectId, out var orgId))
            {
                filter = Builders<AdminProductDocument>.Filter.Eq(x => x.OrganizationId, orgId);
            }

            return await collection.Find(filter).ToListAsync();
        }

        public async Task<List<AdminInventoryDocument>> GetInventoryAsync()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<AdminInventoryDocument>("admininventories");

            var filter = Builders<AdminInventoryDocument>.Filter.Empty;
            if (ObjectId.TryParse(_session.OrganizationObjectId, out var orgId))
            {
                filter = Builders<AdminInventoryDocument>.Filter.Eq(x => x.OrganizationId, orgId);
            }

            return await collection.Find(filter).ToListAsync();
        }
    }
}
