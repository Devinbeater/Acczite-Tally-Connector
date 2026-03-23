using System;

namespace Acczite20.Models
{
    public class Payslip : BaseEntity
    {
        public Guid PayrollId { get; set; }
        public DateTimeOffset GeneratedDate { get; set; }
        public string PDFPath { get; set; }
        public bool EmailSent { get; set; }

        public virtual Payroll Payroll { get; set; }
    }
}
