using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Acczite20.MongoDocuments
{
    public class PayrollDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public string Id { get; set; }

        [BsonElement("organizationId")]
        public string OrganizationId { get; set; }

        [BsonElement("employeeId")]
        public string EmployeeId { get; set; }

        [BsonElement("employeeName")]
        public string EmployeeName { get; set; }

        [BsonElement("month")]
        public string Month { get; set; }

        [BsonElement("basic")]
        public decimal Basic { get; set; }

        [BsonElement("allowances")]
        public decimal Allowances { get; set; }

        [BsonElement("deductions")]
        public decimal Deductions { get; set; }

        [BsonElement("netPay")]
        public decimal NetPay { get; set; }
    }
}
