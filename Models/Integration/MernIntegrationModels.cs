using System;

namespace Acczite20.Models.Integration
{
    public class MernMapping : BaseEntity
    {
        public string EntityType { get; set; } // "Product", "Employee", "Ledger"
        public string MernId { get; set; }     // MongoDB ObjectId
        public string TallyMasterId { get; set; } // Tally GUID or Name
        public Guid LocalId { get; set; }      // Local SQL Guid
        public DateTimeOffset? LastSyncAt { get; set; }
    }

    public class MernProduct : BaseEntity
    {
        public string MernId { get; set; }
        public string SKU { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public decimal Price { get; set; }
        public string UOM { get; set; }
        public string TallyStockItemName { get; set; }
    }

    public class MernEmployee : BaseEntity
    {
        public string MernId { get; set; }
        public string EmployeeNo { get; set; }
        public string FullName { get; set; }
        public string Email { get; set; }
        public string Department { get; set; }
        public string Designation { get; set; }
        public string TallyEmployeeName { get; set; }
    }

    public class MernAttendance : BaseEntity
    {
        public string MernId { get; set; }
        public string EmployeeMernId { get; set; }
        public DateTime CheckIn { get; set; }
        public DateTime? CheckOut { get; set; }
        public string Status { get; set; } // Present, Absent, HalfDay
        public double TotalHours { get; set; }
    }
}
