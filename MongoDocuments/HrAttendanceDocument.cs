using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class HrAttendanceDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("employee")]
        public ObjectId Employee { get; set; }

        [BsonElement("employeeCode")]
        public string EmployeeCode { get; set; }

        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("department")]
        public string Department { get; set; }

        [BsonElement("date")]
        public DateTime Date { get; set; }

        [BsonElement("period")]
        public string Period { get; set; }

        [BsonElement("status")]
        public string Status { get; set; }

        [BsonElement("checkIn")]
        public DateTime? CheckIn { get; set; }

        [BsonElement("checkOut")]
        public DateTime? CheckOut { get; set; }

        [BsonElement("hoursWorked")]
        public string HoursWorked { get; set; }

        [BsonElement("organizationAssigned")]
        public ObjectId OrganizationAssigned { get; set; }
    }
}
