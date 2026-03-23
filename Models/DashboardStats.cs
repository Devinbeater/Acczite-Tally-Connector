using System;
using System.Collections.Generic;

namespace Acczite20.Models
{
    public class DashboardStats
    {
        public int TotalVouchers { get; set; }
        public int TotalLedgers { get; set; }
        public int StockItemCount { get; set; }
        public int SyncedToday { get; set; }
        
        public List<DailyVoucherVolume> VoucherVolumes { get; set; } = new();
        public List<GstDistribution> GstDistributions { get; set; } = new();
        public List<InventoryInsight> InventoryInsights { get; set; } = new();
    }

    public class DailyVoucherVolume
    {
        public DateTime Date { get; set; }
        public int Count { get; set; }
    }

    public class GstDistribution
    {
        public string TaxType { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
    }

    public class InventoryInsight
    {
        public string ItemName { get; set; } = string.Empty;
        public decimal QuantitySold { get; set; }
    }
}
