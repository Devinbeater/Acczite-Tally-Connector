using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Acczite20.Data;
using Acczite20.Models;
using Microsoft.EntityFrameworkCore;

namespace Acczite20.Services.Reports
{
    public class GstSummaryModel
    {
        public decimal TotalTaxableValue { get; set; }
        public decimal TotalCgst { get; set; }
        public decimal TotalSgst { get; set; }
        public decimal TotalIgst { get; set; }
        public decimal TotalTaxAmount => TotalCgst + TotalSgst + TotalIgst;
    }

    public class GstRateWiseSummary
    {
        public decimal TaxRate { get; set; }
        public decimal TaxableValue { get; set; }
        public decimal CgstAmount { get; set; }
        public decimal SgstAmount { get; set; }
        public decimal IgstAmount { get; set; }
        public decimal TotalTax => CgstAmount + SgstAmount + IgstAmount;
    }

    public class GstReportService
    {
        private readonly AppDbContext _dbContext;

        public GstReportService(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task<GstSummaryModel> GetGstSummaryAsync(Guid orgId, DateTime fromDate, DateTime toDate)
        {
            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
            {
                return new GstSummaryModel();
            }

            var breakdowns = await _dbContext.GstBreakdowns
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted && 
                            g.Voucher.VoucherDate >= fromDate && g.Voucher.VoucherDate <= toDate)
                .ToListAsync();

            var summary = new GstSummaryModel
            {
                TotalTaxableValue = breakdowns.Select(g => g.AssessableValue).Sum(),
                TotalCgst = breakdowns.Where(g => g.TaxType.Contains("CGST")).Sum(g => g.TaxAmount),
                TotalSgst = breakdowns.Where(g => g.TaxType.Contains("SGST")).Sum(g => g.TaxAmount),
                TotalIgst = breakdowns.Where(g => g.TaxType.Contains("IGST")).Sum(g => g.TaxAmount)
            };

            return summary;
        }

        public async Task<List<GstRateWiseSummary>> GetRateWiseSummaryAsync(Guid orgId, DateTime fromDate, DateTime toDate)
        {
            var dbType = Services.SessionManager.Instance.SelectedDatabaseType;
            if (dbType == "MongoDB" || string.IsNullOrWhiteSpace(dbType))
            {
                return new List<GstRateWiseSummary>();
            }

            var breakdowns = await _dbContext.GstBreakdowns
                .IgnoreQueryFilters()
                .Where(g => g.OrganizationId == orgId && !g.IsDeleted && 
                            g.Voucher.VoucherDate >= fromDate && g.Voucher.VoucherDate <= toDate)
                .Include(g => g.Voucher)
                .ToListAsync();

            return breakdowns.GroupBy(g => g.TaxRate)
                .Select(group => new GstRateWiseSummary
                {
                    TaxRate = group.Key,
                    TaxableValue = group.Select(g => g.AssessableValue).Distinct().Sum(),
                    CgstAmount = group.Where(g => g.TaxType.Contains("CGST")).Sum(g => g.TaxAmount),
                    SgstAmount = group.Where(g => g.TaxType.Contains("SGST")).Sum(g => g.TaxAmount),
                    IgstAmount = group.Where(g => g.TaxType.Contains("IGST")).Sum(g => g.TaxAmount)
                })
                .OrderBy(r => r.TaxRate)
                .ToList();
        }
    }
}
