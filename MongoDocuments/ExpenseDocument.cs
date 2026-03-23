using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Acczite20.MongoDocuments
{
    public class ExpenseDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public string Id { get; set; }

        [BsonElement("organizationId")]
        public string OrganizationId { get; set; }

        [BsonElement("category")]
        public string Category { get; set; }

        [BsonElement("amount")]
        public decimal Amount { get; set; }

        [BsonElement("voucherNumber")]
        public string VoucherNumber { get; set; }

        [BsonElement("date")]
        public string Date { get; set; }
    }
}
