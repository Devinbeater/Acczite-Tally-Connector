using System;

namespace Acczite20.Models
{
    /// <summary>
    /// Stores which Tally collections are enabled for sync per organization.
    /// </summary>
    public class TallySyncConfiguration : BaseEntity
    {
        public string CollectionName { get; set; } = string.Empty;
        public bool IsEnabled { get; set; }
        public int SyncOrder { get; set; }
    }
}
