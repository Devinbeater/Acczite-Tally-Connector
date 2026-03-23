using MongoDB.Driver;
using System;
using Acczite20.Services;
using Acczite20.Models;

namespace Acczite20.Data
{
    public class MongoRepository
    {
        private readonly IMongoDatabase _database;

        public MongoRepository(IMongoClient client, SessionManager session)
        {
            if (session.OrganizationId == Guid.Empty)
            {
                throw new InvalidOperationException("Cannot create MongoRepository before session is initialized.");
            }

            // Optional: You could ensure names are valid by replacing hyphens if necessary
            var dbName = !string.IsNullOrEmpty(session.DatabaseName) ? session.DatabaseName : "acczite_master";
            _database = client.GetDatabase(dbName);
        }

        public IMongoDatabase Database => _database;

        public IMongoCollection<Acczite20.MongoDocuments.VoucherDocument> Vouchers => _database.GetCollection<Acczite20.MongoDocuments.VoucherDocument>("vouchers");
        public IMongoCollection<Acczite20.MongoDocuments.PayrollDocument> Payroll => _database.GetCollection<Acczite20.MongoDocuments.PayrollDocument>("payroll");
    }
}
