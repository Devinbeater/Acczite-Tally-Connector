using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class AdminProductDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("sku")]
        public string Sku { get; set; }

        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("category")]
        public string Category { get; set; }

        [BsonElement("uom")]
        public string Uom { get; set; }

        [BsonElement("gstPercentage")]
        public decimal GstPercentage { get; set; }

        [BsonElement("organizationId")]
        public ObjectId OrganizationId { get; set; }

        [BsonElement("isActive")]
        public bool IsActive { get; set; }
    }
}
