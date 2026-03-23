using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Acczite20.MongoDocuments
{
    [BsonIgnoreExtraElements]
    public class MongoUserDocument
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("email")]
        public string Email { get; set; }

        [BsonElement("phone")]
        public string Phone { get; set; }

        [BsonElement("password")]
        public string Password { get; set; }

        [BsonElement("organizationAssigned")]
        public ObjectId OrganizationAssigned { get; set; }
        
        [BsonElement("permissions")]
        public List<PermissionDocument> Permissions { get; set; } = new List<PermissionDocument>();
    }

    public class PermissionDocument
    {
        [BsonElement("sectionKey")]
        public string SectionKey { get; set; }
        
        [BsonElement("actions")]
        public List<string> Actions { get; set; } = new List<string>();
    }
}
