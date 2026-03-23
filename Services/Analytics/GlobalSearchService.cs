using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Analytics
{
    public class GlobalSearchResult
    {
        public string Title { get; set; }
        public string SubTitle { get; set; }
        public string Category { get; set; } // Customer, Vendor, Product, Employee, Voucher
        public string DetailActionId { get; set; }
        public DateTime? LastActivity { get; set; }
    }

    public interface IGlobalSearchService
    {
        Task<List<GlobalSearchResult>> SearchAsync(Guid orgId, string query);
    }

    public class GlobalSearchService : IGlobalSearchService
    {
        private readonly AppDbContext _context;

        public GlobalSearchService(AppDbContext context)
        {
            _context = context;
        }

        public async Task<List<GlobalSearchResult>> SearchAsync(Guid orgId, string query)
        {
            if (string.IsNullOrWhiteSpace(query) || query.Length < 3) 
                return new List<GlobalSearchResult>();

            var results = new List<GlobalSearchResult>();
            string lowerQuery = query.ToLower();

            // 1. Search Ledgers (Customers/Vendors/Internal)
            var ledgers = await _context.DimLedgers
                .Where(l => l.OrganizationId == orgId && l.LedgerName.ToLower().Contains(lowerQuery))
                .Take(5)
                .ToListAsync();

            foreach (var l in ledgers)
            {
                var isDebtor = l.LedgerName.Contains("Debtor"); // basic mock
                var isCreditor = l.LedgerName.Contains("Creditor"); // basic mock
                // Optionally calculate outstanding if needed, but better to fetch on demand
                results.Add(new GlobalSearchResult
                {
                    Title = l.LedgerName,
                    SubTitle = $"Group: {l.GroupId}",
                    Category = isDebtor ? "Customer" : 
                               isCreditor ? "Vendor" : "Ledger",
                    DetailActionId = $"ledger:{l.Id}"
                });
            }

            // 2. Search Products (Tally + MERN mappings)
            var products = await _context.DimStockItems
                .Where(p => p.OrganizationId == orgId && p.StockItemName.ToLower().Contains(lowerQuery))
                .Take(5)
                .ToListAsync();

            foreach (var p in products)
            {
                var cloudLinked = await _context.MernMappings.AnyAsync(m => m.OrganizationId == orgId && m.TallyMasterId == p.TallyMasterId && m.EntityType == "Product");
                
                results.Add(new GlobalSearchResult
                {
                    Title = p.StockItemName,
                    SubTitle = cloudLinked ? "Cloud Integrated Product" : "Local Stock Item",
                    Category = "Product",
                    DetailActionId = $"product:{p.Id}"
                });
            }

            // 3. Search Vouchers (by number or narration)
            var vouchers = await _context.FactVouchers
                .Where(v => v.CompanyId == orgId && v.VoucherNumber.ToLower().Contains(lowerQuery)) // CompanyId instead of OrganizationId
                .Take(5)
                .ToListAsync();

            foreach (var v in vouchers)
            {
                results.Add(new GlobalSearchResult
                {
                    Title = $"Voucher #{v.VoucherNumber}",
                    SubTitle = $"Date: {v.VoucherDate.ToString("yyyy-MM-dd")}", // Removed VoucherTypeName to avoid join for now
                    Category = "Transaction",
                    LastActivity = v.VoucherDate.DateTime,
                    DetailActionId = $"voucher:{v.Id}"
                });
            }

            // 4. Search Employees (HR Cloud)
            var employees = await _context.MernEmployees
                .Where(e => e.OrganizationId == orgId && e.TallyEmployeeName.ToLower().Contains(lowerQuery))
                .Take(5)
                .ToListAsync();

            foreach(var e in employees)
            {
                results.Add(new GlobalSearchResult
                {
                    Title = e.TallyEmployeeName,
                    SubTitle = "Cloud HR Employee",
                    Category = "Employee",
                    DetailActionId = $"employee:{e.MernId}"
                });
            }

            return results.OrderBy(r => r.Title).ToList();
        }
    }
}
