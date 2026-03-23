using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;

using Microsoft.EntityFrameworkCore;
using Acczite20.Data;
using Acczite20.Services;
using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public class ReportRow
    {
        public string Timestamp { get; set; } = "";
        public string Collection { get; set; } = "";
        public int RecordCount { get; set; }
        public string Status { get; set; } = "";
        public string Direction { get; set; } = "";
        public string Details { get; set; } = "";
    }

    public partial class ReportsPage : Page
    {
        private List<ReportRow> _reportData = new();
        private readonly AppDbContext _dbContext;
        private readonly INavigationService _navigationService;

        public ReportsPage(AppDbContext dbContext, INavigationService navigationService)
        {
            _dbContext = dbContext;
            _navigationService = navigationService;
            InitializeComponent();
            FromDatePicker.SelectedDate = DateTime.Now.AddDays(-7);
            ToDatePicker.SelectedDate = DateTime.Now;
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack) _navigationService.GoBack();
        }

        private void Variance_Click(object sender, RoutedEventArgs e)
        {
            _navigationService.NavigateTo<ComparativePandLPage>();
        }

        private void Anomaly_Click(object sender, RoutedEventArgs e)
        {
            _navigationService.NavigateTo<AnomalyDetectionPage>();
        }

        private void GenerateReport_Click(object sender, RoutedEventArgs e)
        {
            var reportType = (ReportTypeBox.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "Sync History";
            var from = FromDatePicker.SelectedDate ?? DateTime.Now.AddDays(-7);
            var to = ToDatePicker.SelectedDate ?? DateTime.Now;

            _reportData.Clear();

            switch (reportType)
            {
                case "Sync History":
                    _reportData = GenerateSyncHistory(from, to);
                    break;
                case "Ledger Summary":
                    _reportData = GenerateLedgerSummary(from, to);
                    break;
                case "Voucher Register":
                    _reportData = GenerateVoucherRegister(from, to);
                    break;
                case "Error Log":
                    _reportData = GenerateErrorLog(from, to);
                    break;
                case "Collection Overview":
                    _reportData = GenerateCollectionOverview();
                    break;
            }

            ReportDataGrid.ItemsSource = _reportData;

            // Update stats
            StatTotalRecords.Text = _reportData.Count.ToString();
            StatSyncedOk.Text = _reportData.Count(r => r.Status == "✅ Success").ToString();
            StatErrors.Text = _reportData.Count(r => r.Status == "❌ Failed").ToString();
            StatLastGenerated.Text = DateTime.Now.ToString("hh:mm tt");
            FooterLabel.Text = $"Showing {_reportData.Count} records for \"{reportType}\" ({from:dd/MM/yyyy} — {to:dd/MM/yyyy})";
        }

        private void ExportCsv_Click(object sender, RoutedEventArgs e)
        {
            if (_reportData.Count == 0)
            {
                MessageBox.Show("Generate a report first before exporting.", "No Data", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var dlg = new SaveFileDialog
            {
                Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*",
                FileName = $"Acczite_Report_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                DefaultExt = ".csv"
            };

            if (dlg.ShowDialog() == true)
            {
                try
                {
                    var sb = new StringBuilder();
                    sb.AppendLine("Timestamp,Collection,Records,Status,Direction,Details");

                    foreach (var row in _reportData)
                    {
                        sb.AppendLine($"\"{row.Timestamp}\",\"{row.Collection}\",{row.RecordCount},\"{row.Status}\",\"{row.Direction}\",\"{row.Details}\"");
                    }

                    File.WriteAllText(dlg.FileName, sb.ToString(), Encoding.UTF8);
                    MessageBox.Show($"Report exported successfully!\n\n{dlg.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Export failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        // ══ REPORT GENERATORS ══
        private List<ReportRow> GenerateSyncHistory(DateTime from, DateTime to)
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                return _dbContext.SyncLogs
                    .IgnoreQueryFilters()
                    .Where(x => x.OrganizationId == orgId && x.Timestamp.Date >= from.Date && x.Timestamp.Date <= to.Date)
                    .OrderByDescending(x => x.Timestamp)
                    .Take(1000)
                    .AsEnumerable() // Pull to memory to handle ToString and complex mappings
                    .Select(x => new ReportRow
                    {
                        Timestamp = x.Timestamp.ToString("yyyy-MM-dd HH:mm:ss"),
                        Collection = x.EntityType ?? "General",
                        RecordCount = 1,
                        Status = x.Level == "ERROR" ? "❌ Failed" : "✅ Success",
                        Direction = "Tally → DB",
                        Details = x.Message
                    })
                    .ToList();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error loading logs: " + ex.Message);
                return new List<ReportRow>();
            }
        }

        private List<ReportRow> GenerateLedgerSummary(DateTime from, DateTime to)
        {
            var ledgers = new[] { "Cash", "Sales Account", "Purchase Account", "Bank A/c", "Sundry Debtors", "Sundry Creditors", "Capital", "Duties & Taxes" };
            var rng = new Random();
            return ledgers.Select(l => new ReportRow
            {
                Timestamp = DateTime.Now.ToString("yyyy-MM-dd"),
                Collection = l,
                RecordCount = rng.Next(5, 200),
                Status = "✅ Success",
                Direction = "Tally → DB",
                Details = $"Balance: ₹{rng.Next(10000, 500000):N0}"
            }).ToList();
        }

        private List<ReportRow> GenerateVoucherRegister(DateTime from, DateTime to)
        {
            var types = new[] { "Sales", "Purchase", "Receipt", "Payment", "Journal", "Contra", "Credit Note", "Debit Note" };
            var rng = new Random();
            return types.Select(t => new ReportRow
            {
                Timestamp = from.ToString("yyyy-MM-dd"),
                Collection = t,
                RecordCount = rng.Next(20, 1000),
                Status = "✅ Success",
                Direction = "Tally → DB",
                Details = $"Total: ₹{rng.Next(50000, 2000000):N0}"
            }).ToList();
        }

        private List<ReportRow> GenerateErrorLog(DateTime from, DateTime to)
        {
            return new List<ReportRow>
            {
                new() { Timestamp = from.AddHours(2).ToString("yyyy-MM-dd HH:mm"), Collection = "Day Book", RecordCount = 0, Status = "❌ Failed", Direction = "Tally → DB", Details = "HTTP 500: Tally internal error" },
                new() { Timestamp = from.AddDays(1).ToString("yyyy-MM-dd HH:mm"), Collection = "List of Stock Items", RecordCount = 0, Status = "❌ Failed", Direction = "Tally → DB", Details = "Connection timeout after 5s" },
                new() { Timestamp = from.AddDays(3).ToString("yyyy-MM-dd HH:mm"), Collection = "Voucher Register", RecordCount = 45, Status = "⚠ Partial", Direction = "Tally → DB", Details = "45 of 120 records synced before timeout" },
            };
        }

        private List<ReportRow> GenerateCollectionOverview()
        {
            var cols = new[] { "Ledgers", "Groups", "Voucher Types", "Stock Items", "Stock Groups", "Cost Centres", "Currencies", "Units", "Godowns", "Employees" };
            var rng = new Random();
            return cols.Select(c => new ReportRow
            {
                Timestamp = DateTime.Now.ToString("yyyy-MM-dd"),
                Collection = c,
                RecordCount = rng.Next(5, 300),
                Status = "✅ Active",
                Direction = "Both",
                Details = $"Last sync: {DateTime.Now.AddHours(-rng.Next(1, 48)):g}"
            }).ToList();
        }
    }
}
