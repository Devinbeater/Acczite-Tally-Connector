using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class HrPayrollDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("employee")]
        public ObjectId Employee { get; set; }

        [BsonElement("period")]
        public string Period { get; set; }

        [BsonElement("organizationAssigned")]
        public ObjectId OrganizationAssigned { get; set; }

        [BsonElement("salaryStructure")]
        public SalaryStructureDocument SalaryStructure { get; set; } = new SalaryStructureDocument();

        [BsonElement("deductions")]
        public DeductionsDocument Deductions { get; set; } = new DeductionsDocument();

        [BsonElement("gross")]
        public decimal Gross { get; set; }

        [BsonElement("totalDeductions")]
        public decimal TotalDeductions { get; set; }

        [BsonElement("net")]
        public decimal Net { get; set; }

        [BsonElement("status")]
        public string Status { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class SalaryStructureDocument
    {
        [BsonElement("basic")]
        public decimal Basic { get; set; }

        [BsonElement("hra")]
        public decimal Hra { get; set; }

        [BsonElement("allowances")]
        public object Allowances { get; set; }
    }

    [BsonIgnoreExtraElements]
    public class DeductionsDocument
    {
        [BsonElement("pf")]
        public decimal Pf { get; set; }

        [BsonElement("tax")]
        public decimal Tax { get; set; }

        [BsonElement("tds")]
        public decimal Tds { get; set; }

        [BsonElement("other")]
        public decimal Other { get; set; }
    }
}
