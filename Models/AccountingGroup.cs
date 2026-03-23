using System;

namespace Acczite20.Models
{
    public class AccountingGroup : BaseEntity
    {
        public string Name { get; set; } = string.Empty;
        public string Parent { get; set; } = string.Empty;

        /// <summary>
        /// Tally NATUREOFGROUP: Assets, Liabilities, Income, Expenditure.
        /// This is the single most important field for P&L vs Balance Sheet classification.
        /// </summary>
        public string NatureOfGroup { get; set; } = string.Empty;

        /// <summary>
        /// True if this is one of Tally's 28 predefined primary groups.
        /// </summary>
        public bool IsPrimary { get; set; }

        /// <summary>
        /// Tally AFFECTSGROSSPROFIT: determines Direct vs Indirect classification.
        /// Direct Expenses/Incomes affect Gross Profit; Indirect do not.
        /// </summary>
        public bool AffectsGrossProfit { get; set; }

        /// <summary>
        /// Tally ISADDABLE: whether amounts in this group are additive for consolidation.
        /// </summary>
        public bool IsAddable { get; set; }

        public string TallyMasterId { get; set; } = string.Empty;
    }
}
