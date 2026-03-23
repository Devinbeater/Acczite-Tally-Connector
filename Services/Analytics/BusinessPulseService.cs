using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models.Analytics;
using Acczite20.Models.Warehouse;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Analytics
{
    public interface IBusinessPulseService
    {
        Task<BusinessPulseStats> GetDailyPulseAsync(Guid orgId);
        Task UpdatePulseSnapshotAsync(Guid orgId);
    }

    public class BusinessPulseService : IBusinessPulseService
    {
        private readonly AppDbContext _context;
        private readonly IFinancialAnalysisService _analysisService;

        public BusinessPulseService(AppDbContext context, IFinancialAnalysisService analysisService)
        {
            _context = context;
            _analysisService = analysisService;
        }

        public async Task<BusinessPulseStats> GetDailyPulseAsync(Guid orgId)
        {
            var stats = new BusinessPulseStats();
            var today = DateTimeOffset.UtcNow.Date;

            try
            {
                // 1. Try to get from Snapshot Cache first for performance
                var snapshot = await _context.BusinessPulseSnapshots
                    .OrderByDescending(s => s.Date)
                    .FirstOrDefaultAsync(s => s.OrganizationId == orgId && s.Date == today);

                if (snapshot != null)
                {
                    stats.SalesToday = snapshot.SalesToday;
                    stats.InvoicesToday = snapshot.InvoicesToday;
                    stats.CollectionsToday = snapshot.CollectionsToday;
                    stats.TotalReceivables = snapshot.Receivables;
                    stats.TotalPayables = snapshot.Payables;
                    stats.NetCashBank = snapshot.CashPosition;
                }
                else
                {
                    // Fallback to real-time calculation if snapshot doesn't exist for today
                    stats = await CalculateRealTimePulse(orgId);
                }

                // 2. Anomaly & Alerts (Computed dynamically or from service)
                var anomalies = await _analysisService.GetAnomaliesAsync(orgId);
                foreach (var a in anomalies.Take(5))
                {
                    stats.Alerts.Add(new PulseAlert { 
                        Message = a.Description, 
                        Severity = a.Severity, 
                        Category = a.Title.Contains("Expense") ? "Expenses" : "Risk" 
                    });
                }

                // Receivable Risk Alert
                if (stats.TotalReceivables > stats.NetCashBank * 2 && stats.NetCashBank > 0)
                {
                    stats.Alerts.Add(new PulseAlert { 
                        Message = "Receivables exceed 2x cash - High liquidity risk.", 
                        Severity = "High", 
                        Category = "Cash Flow" 
                    });
                }

            }
            catch (Exception ex)
            {
                stats.Alerts.Add(new PulseAlert { Message = $"Pulse Error: {ex.Message}", Severity = "Critical", Category = "System" });
            }

            return stats;
        }

        public async Task UpdatePulseSnapshotAsync(Guid orgId)
        {
            var stats = await CalculateRealTimePulse(orgId);
            var today = DateTimeOffset.UtcNow.Date;

            var existing = await _context.BusinessPulseSnapshots
                .FirstOrDefaultAsync(s => s.OrganizationId == orgId && s.Date == today);

            if (existing == null)
            {
                _context.BusinessPulseSnapshots.Add(new BusinessPulseSnapshot
                {
                    OrganizationId = orgId,
                    Date = today,
                    SalesToday = stats.SalesToday,
                    InvoicesToday = stats.InvoicesToday,
                    CollectionsToday = stats.CollectionsToday,
                    Receivables = stats.TotalReceivables,
                    Payables = stats.TotalPayables,
                    CashPosition = stats.NetCashBank,
                    LastSyncedAt = DateTime.UtcNow
                });
            }
            else
            {
                existing.SalesToday = stats.SalesToday;
                existing.InvoicesToday = stats.InvoicesToday;
                existing.CollectionsToday = stats.CollectionsToday;
                existing.Receivables = stats.TotalReceivables;
                existing.Payables = stats.TotalPayables;
                existing.CashPosition = stats.NetCashBank;
                existing.LastSyncedAt = DateTime.UtcNow;
            }

            await _context.SaveChangesAsync();
        }

        private async Task<BusinessPulseStats> CalculateRealTimePulse(Guid orgId)
        {
            var stats = new BusinessPulseStats();
            var today = DateTimeOffset.UtcNow.Date;

            // Use Mapping Layer for Ledger IDs
            var mappings = await _context.LedgerMappings
                .Where(m => m.OrganizationId == orgId)
                .ToListAsync();

            var salesLedgerIds = mappings.Where(m => m.MappedCategory == "Sales").Select(m => m.LedgerId).ToList();
            var debilityLedgerIds = mappings.Where(m => m.MappedCategory == "Receivables").Select(m => m.LedgerId).ToList();
            var creditLedgerIds = mappings.Where(m => m.MappedCategory == "Payables").Select(m => m.LedgerId).ToList();
            var cashLedgerIds = mappings.Where(m => m.MappedCategory == "Cash").Select(m => m.LedgerId).ToList();

            // Sales Calculation
            var salesEntries = await _context.FactLedgerEntries
                .Where(e => e.OrganizationId == orgId && e.VoucherDate >= today && salesLedgerIds.Contains(e.LedgerId))
                .ToListAsync();

            stats.InvoicesToday = salesEntries.Select(e => e.VoucherId).Distinct().Count();
            stats.SalesToday = salesEntries.Sum(e => e.Credit - e.Debit);

            // Collections (Receipts) - just using Cash/Bank ledgers that receive money today
            stats.CollectionsToday = await _context.FactLedgerEntries
                .Where(e => e.OrganizationId == orgId && e.VoucherDate >= today && cashLedgerIds.Contains(e.LedgerId))
                .SumAsync(e => e.Debit); // Cash hitting debit is collection

            // Receivables & Payables (Using Mappings + Snapshots)
            var currentMonth = DateTime.Now.Month;
            var currentYear = DateTime.Now.Year;
            var currentBalances = await _context.LedgerBalanceSnapshots
                .Where(s => s.OrganizationId == orgId && s.Year == currentYear && s.Month == currentMonth)
                .ToListAsync();

            stats.TotalReceivables = currentBalances.Where(s => debilityLedgerIds.Contains(s.LedgerId)).Sum(s => s.ClosingBalance);
            stats.TotalPayables = currentBalances.Where(s => creditLedgerIds.Contains(s.LedgerId)).Sum(s => Math.Abs(s.ClosingBalance));
            stats.NetCashBank = currentBalances.Where(s => cashLedgerIds.Contains(s.LedgerId)).Sum(s => s.ClosingBalance);

            return stats;
        }
    }
}
