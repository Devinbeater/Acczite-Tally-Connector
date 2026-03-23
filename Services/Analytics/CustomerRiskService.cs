using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Analytics;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Analytics
{
    public interface ICustomerRiskService
    {
        Task<List<CustomerRiskProfile>> GetHighRiskCustomersAsync(Guid organizationId);
        Task<CustomerRiskProfile> GetCustomerRiskProfileAsync(Guid organizationId, Guid ledgerId);
    }

    public class CustomerRiskService : ICustomerRiskService
    {
        private readonly AppDbContext _db;

        public CustomerRiskService(AppDbContext db)
        {
            _db = db;
        }

        public async Task<List<CustomerRiskProfile>> GetHighRiskCustomersAsync(Guid organizationId)
        {
            // For MVP: Get debtors with high outstanding
            var debtors = await _db.DimLedgers
                .Where(l => l.OrganizationId == organizationId && l.ParentGroupName == "Sundry Debtors")
                .ToListAsync();

            var profiles = new List<CustomerRiskProfile>();

            foreach (var debtor in debtors)
            {
                var profile = await CalculateRiskAsync(organizationId, debtor.Id, debtor.LedgerName);
                if (profile.RiskLevel == "High" || profile.RiskLevel == "Critical")
                {
                    profiles.Add(profile);
                }
            }

            return profiles.OrderByDescending(p => p.TotalOutstanding).ToList();
        }

        public async Task<CustomerRiskProfile> GetCustomerRiskProfileAsync(Guid organizationId, Guid ledgerId)
        {
            var ledger = await _db.DimLedgers.FirstOrDefaultAsync(l => l.Id == ledgerId);
            return await CalculateRiskAsync(organizationId, ledgerId, ledger?.LedgerName ?? "Unknown");
        }

        private async Task<CustomerRiskProfile> CalculateRiskAsync(Guid organizationId, Guid ledgerId, string name)
        {
            // Simple logic for now: query snapshots or live entries
            var snapshot = await _db.LedgerBalanceSnapshots
                .Where(s => s.LedgerId == ledgerId && s.OrganizationId == organizationId)
                .OrderByDescending(s => s.Year)
                .ThenByDescending(s => s.Month)
                .FirstOrDefaultAsync();

            decimal outstanding = snapshot?.ClosingBalance ?? 0;
            
            // To be realistic, closing balance for debtors is usually Debit
            // But Tally uses Credits for Sales, Debits for Receipts
            // In our warehouse: Credit = Positive, Debit = Negative (usually)
            // Let's assume outstanding is abs for now
            decimal absOutstanding = Math.Abs(outstanding);

            var profile = new CustomerRiskProfile
            {
                LedgerId = ledgerId,
                CustomerName = name,
                TotalOutstanding = absOutstanding,
                RiskLevel = "Low"
            };

            if (absOutstanding > 1000000) // > 10 L
            {
                profile.RiskLevel = "Critical";
                profile.RiskReason = "Extreme outstanding balance detected.";
                profile.Indicators.Add(new RiskIndicator { Type = "Volume", Status = "Bad", Message = "Exposure exceeds 10 Lakhs" });
            }
            else if (absOutstanding > 500000) // > 5 L
            {
                profile.RiskLevel = "High";
                profile.RiskReason = "High credit exposure.";
                profile.Indicators.Add(new RiskIndicator { Type = "Volume", Status = "Warning", Message = "Exposure exceeds 5 Lakhs" });
            }

            // TODO: Add aging logic by looking at FactVoucher dates vs Receipts
            
            return profile;
        }
    }
}
