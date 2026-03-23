using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Acczite20.MongoDocuments
{
    public class VoucherDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public string Id { get; set; } // E.g. "voucher_2026_001", store SQL Guid as string or a specific format

        [BsonElement("companyId")]
        public string CompanyId { get; set; }

        [BsonElement("organizationId")]
        public string OrganizationId { get; set; }

        [BsonElement("voucherNumber")]
        public string VoucherNumber { get; set; }

        [BsonElement("voucherType")]
        public string VoucherType { get; set; }

        [BsonElement("voucherDate")]
        public string VoucherDate { get; set; }

        [BsonElement("referenceNumber")]
        public string ReferenceNumber { get; set; }

        [BsonElement("totalAmount")]
        public decimal TotalAmount { get; set; }

        [BsonElement("isCancelled")]
        public bool IsCancelled { get; set; }

        [BsonElement("ledgerEntries")]
        public List<LedgerEntryDocument> LedgerEntries { get; set; } = new List<LedgerEntryDocument>();

        [BsonElement("taxDetails")]
        public TaxDetailsDocument TaxDetails { get; set; }

        [BsonElement("lastModified")]
        public string LastModified { get; set; }
    }

    public class LedgerEntryDocument
    {
        [BsonElement("ledgerName")]
        public string LedgerName { get; set; }

        [BsonElement("debit")]
        public decimal Debit { get; set; }

        [BsonElement("credit")]
        public decimal Credit { get; set; }
    }

    public class TaxDetailsDocument
    {
        [BsonElement("cgst")]
        public decimal CGST { get; set; }

        [BsonElement("sgst")]
        public decimal SGST { get; set; }

        [BsonElement("igst")]
        public decimal IGST { get; set; }
    }
}
