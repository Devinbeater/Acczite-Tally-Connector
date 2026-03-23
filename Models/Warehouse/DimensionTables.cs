using System;

namespace Acczite20.Models.Warehouse
{
    public class DimLedger : BaseEntity
    {
        public string LedgerName { get; set; }
        public Guid GroupId { get; set; }
        public string ParentGroupName { get; set; }
        public bool IsBillWise { get; set; }
        public bool IsGstLedger { get; set; }
        public string TallyMasterId { get; set; }
    }

    public class DimGroup : BaseEntity
    {
        public string GroupName { get; set; }
        public Guid? ParentGroupId { get; set; }
        public string RootGroup { get; set; } // Assets, Liabilities, Income, Expenses
        public string TallyMasterId { get; set; }
    }

    public class DimStockItem : BaseEntity
    {
        public string StockItemName { get; set; }
        public Guid GroupId { get; set; }
        public Guid UnitId { get; set; }
        public string HsnCode { get; set; }
        public string TallyMasterId { get; set; }
    }

    public class DimVoucherType : BaseEntity
    {
        public string VoucherTypeName { get; set; }
        public string TallyMasterId { get; set; }
    }
}
