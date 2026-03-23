using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class HrEmployeeDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("employeeCode")]
        public string EmployeeCode { get; set; }

        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("email")]
        public string Email { get; set; }

        [BsonElement("phone")]
        public string Phone { get; set; }

        [BsonElement("department")]
        public string Department { get; set; }

        [BsonElement("designation")]
        public string Designation { get; set; }

        [BsonElement("status")]
        public string Status { get; set; }

        [BsonElement("organizationAssigned")]
        public ObjectId OrganizationAssigned { get; set; }
    }
}
