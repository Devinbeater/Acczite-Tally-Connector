using System;

namespace Acczite20.Models
{
    public class CostCategory : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public bool AffectsStock { get; set; }
        public string? TallyMasterId { get; set; }
    }

    public class CostCentre : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string? CategoryName { get; set; }
        public string? ParentName { get; set; }
        public string? TallyMasterId { get; set; }
    }
}
