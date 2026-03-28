using System;

namespace Acczite20.Models
{
    public class VoucherType : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public string? TallyMasterId { get; set; }
        public long TallyAlterId { get; set; }
    }
}
