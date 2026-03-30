using System;

namespace Acczite20.Models
{
    public class Company : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string TallyCompanyName { get; set; } = string.Empty;
        public string? GSTNumber { get; set; }
        public string? Address { get; set; }
        public bool IsActive { get; set; }
    }
}
