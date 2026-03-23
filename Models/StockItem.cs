using System;
using System.ComponentModel.DataAnnotations;

namespace Acczite20.Models
{
    public class StockItem : BaseEntity
    {
        [Required]
        [MaxLength(255)]
        public string Name { get; set; } = string.Empty;

        [MaxLength(255)]
        public string StockGroup { get; set; } = string.Empty;

        public decimal OpeningBalance { get; set; }
        public decimal ClosingBalance { get; set; }

        public string Description { get; set; } = string.Empty;
        public string BaseUnit { get; set; } = string.Empty;
        public string TallyMasterId { get; set; } = string.Empty;
    }
}
