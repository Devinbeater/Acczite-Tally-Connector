using System;

namespace Acczite20.Models
{
    public class MongoProjectionQueue : BaseEntity
    {
        public string CollectionName { get; set; } = string.Empty;
        public string PayloadJson { get; set; } = string.Empty;
        public int RetryCount { get; set; } = 0;
        public DateTimeOffset? LastAttemptAt { get; set; }
        public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;
    }
}
