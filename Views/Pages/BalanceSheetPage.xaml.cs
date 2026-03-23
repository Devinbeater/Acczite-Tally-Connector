using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    public partial class BalanceSheetPage : Page
    {
        private readonly BalanceSheetService _bsService;

        public BalanceSheetPage(BalanceSheetService bsService)
        {
            InitializeComponent();
            _bsService = bsService;

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

                if (Math.Abs(report.Difference) > 0.01m)
                {
                    // In a real app, we'd add "Difference in Balances" or Profit/Loss to balance it
                    // For now, we'll just show the totals.
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load Balance Sheet: {ex.Message}", "Error");
            }
        }
    }
}
