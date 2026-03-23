using System;
using System.ComponentModel.DataAnnotations;

namespace Acczite20.Models
{
    public class Ledger : BaseEntity
    {
        [Required]
        [MaxLength(255)]
        public string Name { get; set; } = string.Empty;

        [MaxLength(255)]
        public string ParentGroup { get; set; } = string.Empty;

        public decimal OpeningBalance { get; set; }
        public decimal ClosingBalance { get; set; }

        public string Description { get; set; } = string.Empty;
        public string TallyMasterId { get; set; } = string.Empty;

        // --- GST Fields ---
        /// <summary>
        /// Tally GSTAPPLICABILITY: Applicable, Not Applicable, Undefined
        /// </summary>
        public string GSTApplicability { get; set; } = string.Empty;

        /// <summary>
        /// Tally GSTREGISTRATIONTYPE: Regular, Composition, Unregistered, Consumer, etc.
        /// </summary>
        public string GSTRegistrationType { get; set; } = string.Empty;

        /// <summary>
        /// GSTIN / UIN number from Tally
        /// </summary>
        [MaxLength(20)]
        public string GSTIN { get; set; } = string.Empty;

        // --- Party / Billing Fields ---
        /// <summary>
        /// Tally ISBILLWISEON: essential for receivables/payables aging
        /// </summary>
        public bool IsBillWise { get; set; }

        [MaxLength(255)]
        public string Address { get; set; } = string.Empty;

        [MaxLength(100)]
        public string State { get; set; } = string.Empty;

        [MaxLength(20)]
        public string PAN { get; set; } = string.Empty;

        [MaxLength(255)]
        public string Email { get; set; } = string.Empty;

        /// <summary>
        /// Tally MAILINGNAME: display name for the party
        /// </summary>
        [MaxLength(255)]
        public string MailingName { get; set; } = string.Empty;
    }
}
