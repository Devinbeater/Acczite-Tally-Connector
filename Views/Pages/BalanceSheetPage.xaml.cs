using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    // Thin view-model for the diagnostics ItemsControl binding
    internal class DiagnosticRowVm
    {
        public string Description { get; set; } = string.Empty;
        public Brush SeverityColor { get; set; } = Brushes.Gray;
    }

    public partial class BalanceSheetPage : Page
    {
        private readonly BalanceSheetService _bsService;
        private readonly FinancialHealthService _healthService;

        public BalanceSheetPage(BalanceSheetService bsService, FinancialHealthService healthService)
        {
            InitializeComponent();
            _bsService = bsService;
            _healthService = healthService;

            AsOnDatePicker.SelectedDate = DateTime.Today;
            Loaded += BalanceSheetPage_Loaded;
        }

        private async void BalanceSheetPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadReportAsync();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadReportAsync();
        }

        private async Task LoadReportAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var toDate = AsOnDatePicker.SelectedDate ?? DateTime.Today;

                var report = await _bsService.GetBalanceSheetAsync(orgId, toDate);

                LiabilityList.ItemsSource = report.Liabilities;
                AssetList.ItemsSource = report.Assets;

                TxtTotalLiabilities.Text = $"₹ {report.TotalLiabilities:N2}";
                TxtTotalAssets.Text = $"₹ {report.TotalAssets:N2}";

                // Compute and display financial health status
                var fyStart = FinancialHealthService.IndianFYStart(toDate);
                var health = await _healthService.ComputeAsync(orgId, fyStart, toDate);
                UpdateHealthBar(health);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load Balance Sheet: {ex.Message}", "Error");
            }
        }

        private void UpdateHealthBar(FinancialHealthResult health)
        {
            Color bg, dot, text;

            switch (health.BSStatus)
            {
                case HealthStatus.Green:
                    bg   = Color.FromRgb(220, 252, 231); // green-100
                    dot  = Color.FromRgb(21,  128, 61);  // green-700
                    text = Color.FromRgb(21,  128, 61);
                    break;

                case HealthStatus.Yellow:
                    bg   = Color.FromRgb(254, 243, 199); // amber-100
                    dot  = Color.FromRgb(180, 83,  9);   // amber-700
                    text = Color.FromRgb(120, 53,  15);  // amber-900
                    break;

                default: // Red
                    bg   = Color.FromRgb(254, 226, 226); // red-100
                    dot  = Color.FromRgb(185, 28,  28);  // red-700
                    text = Color.FromRgb(153, 27,  27);  // red-800
                    break;
            }

            HealthStatusBar.Background = new SolidColorBrush(bg);
            HealthDot.Fill             = new SolidColorBrush(dot);
            TxtHealthLabel.Foreground  = new SolidColorBrush(text);
            TxtHealthDetail.Foreground = new SolidColorBrush(text);
            TxtHealthDiff.Foreground   = new SolidColorBrush(text);

            TxtHealthLabel.Text  = health.BSStatus switch
            {
                HealthStatus.Green  => "Balanced",
                HealthStatus.Yellow => "Unclosed P&L",
                _                   => "Data Mismatch"
            };

            TxtHealthDetail.Text = health.BSDifferenceLabel;

            TxtHealthDiff.Text = Math.Abs(health.BSDifference) < 0.01m
                ? string.Empty
                : $"Diff: ₹ {Math.Abs(health.BSDifference):N2}";

            // Last verified timestamp
            TxtLastVerified.Text = $"Verified {health.GeneratedAt.ToLocalTime():dd-MMM-yy HH:mm}";

            // ── Drill-down diagnostics ──
            bool hasDiagnostics = health.Diagnostics.Any()
                               || health.TopUnbalancedVouchers.Any()
                               || health.OrphanLedgers.Any();

            DiagnosticsPanel.Visibility = hasDiagnostics ? Visibility.Visible : Visibility.Collapsed;

            if (hasDiagnostics)
            {
                DiagnosticsList.ItemsSource = health.Diagnostics.Select(d => new DiagnosticRowVm
                {
                    Description   = d.Description,
                    SeverityColor = d.Severity == HealthStatus.Red
                        ? new SolidColorBrush(Color.FromRgb(220, 38, 38))   // red-600
                        : new SolidColorBrush(Color.FromRgb(217, 119, 6))   // amber-600
                }).ToList();

                if (health.TopUnbalancedVouchers.Any())
                {
                    UnbalancedVouchersPanel.Visibility = Visibility.Visible;
                    UnbalancedVouchersList.ItemsSource = health.TopUnbalancedVouchers;
                }
                else
                {
                    UnbalancedVouchersPanel.Visibility = Visibility.Collapsed;
                }

                if (health.OrphanLedgers.Any())
                {
                    OrphanLedgersPanel.Visibility = Visibility.Visible;
                    OrphanLedgersList.ItemsSource  = health.OrphanLedgers;
                }
                else
                {
                    OrphanLedgersPanel.Visibility = Visibility.Collapsed;
                }
            }
        }
    }
}
