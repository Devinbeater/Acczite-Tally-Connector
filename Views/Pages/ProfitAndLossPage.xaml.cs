using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
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

            FromDatePicker.SelectedDate = new DateTime(DateTime.Today.Year, 4, 1); // Start of Financial Year
            if (DateTime.Today.Month < 4) FromDatePicker.SelectedDate = FromDatePicker.SelectedDate.Value.AddYears(-1);
            
            ToDatePicker.SelectedDate = DateTime.Today;

            Loaded += ProfitAndLossPage_Loaded;
        }

        private async void ProfitAndLossPage_Loaded(object sender, RoutedEventArgs e)
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
                var fromDate = FromDatePicker.SelectedDate ?? DateTime.Today.AddMonths(-1);
                var toDate = ToDatePicker.SelectedDate ?? DateTime.Today;

                var report = await _plService.GetPandLReportAsync(orgId, fromDate, toDate);

                ExpenseList.ItemsSource = report.Expenses;
                IncomeList.ItemsSource = report.Incomes;

                TxtTotalExpense.Text = $"₹ {report.TotalExpense:N2}";
                TxtTotalIncome.Text = $"₹ {report.TotalIncome:N2}";

                decimal net = report.NetProfit;
                TxtNetPl.Text = $"₹ {Math.Abs(net):N2}";
                
                if (net >= 0)
                {
                    TxtPlLabel.Text = "Net Profit:";
                    TxtNetPl.Foreground = System.Windows.Media.Brushes.White;
                }
                else
                {
                    TxtPlLabel.Text = "Net Loss:";
                    TxtNetPl.Foreground = System.Windows.Media.Brushes.LightPink;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load P&L: {ex.Message}", "Error");
            }
        }
    }
}
