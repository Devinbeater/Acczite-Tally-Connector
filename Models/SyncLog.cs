using System;

namespace Acczite20.Models
{
    public class SyncLog : BaseEntity
    {
        public DateTimeOffset Timestamp { get; set; }
        public string Level { get; set; }
        public string Message { get; set; }
        public string EntityType { get; set; }
        public string RecordReference { get; set; }
    }
}
