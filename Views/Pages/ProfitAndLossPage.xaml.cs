using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;
using Acczite20.Services.Reports;

namespace Acczite20.Views.Pages
{
    public partial class ProfitAndLossPage : Page
    {
        private readonly PandLService _plService;

        public ProfitAndLossPage(PandLService plService)
        {
            InitializeComponent();
            _plService = plService;

            // Default to current Indian financial year (Apr 1 → today)
            var today = DateTime.Today;
            int fyYear = today.Month >= 4 ? today.Year : today.Year - 1;
            FromDatePicker.SelectedDate = new DateTime(fyYear, 4, 1);
            ToDatePicker.SelectedDate   = today;

            Loaded += ProfitAndLossPage_Loaded;
        }

        private async void ProfitAndLossPage_Loaded(object sender, RoutedEventArgs e)
            => await LoadReportAsync();

        private async void Refresh_Click(object sender, RoutedEventArgs e)
            => await LoadReportAsync();

        private async Task LoadReportAsync()
        {
            try
            {
                var orgId    = SessionManager.Instance.OrganizationId;
                var fromDate = FromDatePicker.SelectedDate ?? DateTime.Today.AddMonths(-1);
                var toDate   = ToDatePicker.SelectedDate   ?? DateTime.Today;

                var report = await _plService.GetPandLReportAsync(orgId, fromDate, toDate);

                // ── Bind lists ──────────────────────────────────────────────────
                DirectExpenseList  .ItemsSource = report.DirectExpenses;
                IndirectExpenseList.ItemsSource = report.IndirectExpenses;
                DirectIncomeList   .ItemsSource = report.DirectIncomes;
                IndirectIncomeList .ItemsSource = report.IndirectIncomes;

                // ── Column totals ───────────────────────────────────────────────
                TxtTotalExpense.Text = $"₹ {(report.TotalDirectExpense + report.TotalIndirectExpense):N2}";
                TxtTotalIncome .Text = $"₹ {(report.TotalDirectIncome  + report.TotalIndirectIncome ):N2}";

                // ── Gross Profit / Loss divider ─────────────────────────────────
                var gp = report.GrossProfit;
                if (gp >= 0)
                {
                    // Gross Profit sits on the Expense (left) side — balances trading
                    GpDividerLeft.Background  = new SolidColorBrush(Color.FromRgb(240, 253, 244));
                    GpDividerLeft.BorderBrush = new SolidColorBrush(Color.FromRgb(187, 247, 208));
                    TxtGpDividerLabel.Text      = "Gross Profit";
                    TxtGpDividerLabel.Foreground = new SolidColorBrush(Color.FromRgb(21, 128, 61));
                    TxtGpDividerValue.Text      = $"₹ {gp:N2}";
                    TxtGpDividerValue.Foreground = new SolidColorBrush(Color.FromRgb(21, 128, 61));
                    GpDividerRight.Visibility   = Visibility.Collapsed;
                }
                else
                {
                    // Gross Loss sits on the Income (right) side — balances trading
                    GpDividerLeft.Background  = new SolidColorBrush(Color.FromRgb(254, 242, 242));
                    GpDividerLeft.BorderBrush = new SolidColorBrush(Color.FromRgb(254, 202, 202));
                    TxtGpDividerLabel.Text      = "Gross Loss";
                    TxtGpDividerLabel.Foreground = new SolidColorBrush(Color.FromRgb(185, 28, 28));
                    TxtGpDividerValue.Text      = $"₹ {Math.Abs(gp):N2}";
                    TxtGpDividerValue.Foreground = new SolidColorBrush(Color.FromRgb(185, 28, 28));

                    GpDividerRight.Visibility   = Visibility.Visible;
                    TxtGpRightLabel.Text         = "Gross Loss";
                    TxtGpRightValue.Text         = $"₹ {Math.Abs(gp):N2}";
                }

                // ── Header badges ───────────────────────────────────────────────
                SetGrossProfitBadge(gp);
                SetNetProfitBadge(report.NetProfit);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load P&L: {ex.Message}", "Error",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void SetGrossProfitBadge(decimal gp)
        {
            bool profit = gp >= 0;
            GrossProfitBadge.Background  = profit
                ? new SolidColorBrush(Color.FromRgb(240, 253, 244))
                : new SolidColorBrush(Color.FromRgb(254, 242, 242));
            GrossProfitBadge.SetValue(Border.BorderBrushProperty, profit
                ? new SolidColorBrush(Color.FromRgb(187, 247, 208))
                : new SolidColorBrush(Color.FromRgb(254, 202, 202)));
            GrossProfitBadge.BorderThickness = new Thickness(1);

            var fg = profit
                ? new SolidColorBrush(Color.FromRgb(21, 128, 61))
                : new SolidColorBrush(Color.FromRgb(185, 28, 28));
            TxtGPLabel.Text      = profit ? "Gross Profit" : "Gross Loss";
            TxtGPLabel.Foreground = fg;
            TxtGrossProfit.Text  = $"₹ {Math.Abs(gp):N2}";
            TxtGrossProfit.Foreground = fg;
        }

        private void SetNetProfitBadge(decimal np)
        {
            bool profit = np >= 0;
            NetProfitBadge.Background = profit
                ? new SolidColorBrush(Color.FromRgb(20, 184, 166))   // teal
                : new SolidColorBrush(Color.FromRgb(220, 38, 38));    // red
            NetProfitBadge.BorderThickness = new Thickness(0);

            TxtNPLabel.Text       = profit ? "Net Profit" : "Net Loss";
            TxtNPLabel.Foreground = Brushes.White;
            TxtNetProfit.Text     = $"₹ {Math.Abs(np):N2}";
            TxtNetProfit.Foreground = Brushes.White;
        }
    }
}
