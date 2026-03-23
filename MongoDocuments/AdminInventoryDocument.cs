using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class AdminInventoryDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("productId")]
        public ObjectId ProductId { get; set; }

        [BsonElement("availableStock")]
        public decimal AvailableStock { get; set; }

        [BsonElement("costPrice")]
        public decimal CostPrice { get; set; }

        [BsonElement("mrp")]
        public decimal? Mrp { get; set; }

        [BsonElement("organizationId")]
        public ObjectId OrganizationId { get; set; }

        [BsonElement("batch")]
        public string Batch { get; set; }

        [BsonElement("lastMovement")]
        public DateTime? LastMovement { get; set; }
    }
}
