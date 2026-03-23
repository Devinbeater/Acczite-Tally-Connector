using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Acczite20.MongoDocuments;
using Acczite20.Data;
using MongoDB.Bson;

namespace Acczite20.Services
{
    public class HrService
    {
        private readonly IMongoClient _client;
        private readonly SessionManager _session;
        private readonly MongoService _mongoService;

        public HrService(IMongoClient client, SessionManager session, MongoService mongoService)
        {
            _client = client;
            _session = session;
            _mongoService = mongoService;
        }

        private IMongoDatabase GetDatabase() => _mongoService.GetDatabase();

        public async Task<List<HrAttendanceDocument>> GetAttendanceAsync()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<HrAttendanceDocument>("attendances");
            
            var filter = Builders<HrAttendanceDocument>.Filter.Empty;
            if (ObjectId.TryParse(_session.OrganizationObjectId, out var orgId))
            {
                filter = Builders<HrAttendanceDocument>.Filter.Eq(x => x.OrganizationAssigned, orgId);
            }

            return await collection.Find(filter).SortByDescending(x => x.Date).Limit(500).ToListAsync();
        }

        public async Task<List<HrPayrollDocument>> GetPayrollAsync()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<HrPayrollDocument>("payrolls");

            var filter = Builders<HrPayrollDocument>.Filter.Empty;
            if (ObjectId.TryParse(_session.OrganizationObjectId, out var orgId))
            {
                filter = Builders<HrPayrollDocument>.Filter.Eq(x => x.OrganizationAssigned, orgId);
            }

            return await collection.Find(filter).SortByDescending(x => x.Period).Limit(500).ToListAsync();
        }

        public async Task<List<HrEmployeeDocument>> GetEmployeesAsync()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<HrEmployeeDocument>("employees");

            var filter = Builders<HrEmployeeDocument>.Filter.Empty;
            if (ObjectId.TryParse(_session.OrganizationObjectId, out var orgId))
            {
                filter = Builders<HrEmployeeDocument>.Filter.Eq(x => x.OrganizationAssigned, orgId);
            }

            return await collection.Find(filter).ToListAsync();
        }
    }
}
