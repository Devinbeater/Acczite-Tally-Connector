using System;

namespace Acczite20.Models
{
    public class Payroll : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string EmployeeName { get; set; }
        public string EmployeeId { get; set; }
        public decimal BasicSalary { get; set; }
        public decimal Allowances { get; set; }
        public decimal Deductions { get; set; }
        public decimal NetPay { get; set; }
        public string PayrollMonth { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
