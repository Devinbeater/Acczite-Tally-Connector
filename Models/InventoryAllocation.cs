using System;

namespace Acczite20.Models
{
    public class InventoryAllocation : BaseEntity
    {
        public Guid VoucherId { get; set; }
        public string StockItemName { get; set; } = string.Empty;
        public decimal ActualQuantity { get; set; }
        public decimal BilledQuantity { get; set; }
        public decimal Rate { get; set; }
        public decimal Amount { get; set; }

        public virtual Voucher Voucher { get; set; }
    }
}
