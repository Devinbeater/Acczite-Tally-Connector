using System;

namespace Acczite20.Models
{
    public class StockCategory : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string? Parent { get; set; } = string.Empty;
        public string? TallyMasterId { get; set; }
        public long TallyAlterId { get; set; }
    }
}
