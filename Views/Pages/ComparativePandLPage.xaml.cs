using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Analytics;
using Acczite20.Models.Analytics;

namespace Acczite20.Views.Pages
{
    public partial class ComparativePandLPage : Page
    {
        private readonly IFinancialAnalysisService _analysisService;

        public ComparativePandLPage(IFinancialAnalysisService analysisService)
        {
            InitializeComponent();
            _analysisService = analysisService;
            Loaded += ComparativePandLPage_Loaded;
        }

        private async void ComparativePandLPage_Loaded(object sender, RoutedEventArgs e)
        {
            await RunAnalysisAsync();
        }

        private async void Analyze_Click(object sender, RoutedEventArgs e)
        {
            await RunAnalysisAsync();
        }

        private async Task RunAnalysisAsync()
        {
            try
            {
                if (!int.TryParse(TxtYear1.Text, out int y1)) y1 = DateTime.Today.Year - 1;
                if (!int.TryParse(TxtYear2.Text, out int y2)) y2 = DateTime.Today.Year;

                var orgId = SessionManager.Instance.OrganizationId;
                var data = await _analysisService.GetComparativeProfitLossAsync(orgId, y1, y2);
                
                VarianceGrid.ItemsSource = data;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Analysis failed: {ex.Message}");
            }
        }

        private void VarianceGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (VarianceGrid.SelectedItem is VarianceReportRow row)
            {
                // Drill Down logic (Point 2)
                MessageBox.Show($"Selected ledger row: {row.LedgerName}. Navigation handled by MainWindow.", "Ledger Navigation");
            }
        }
    }
}
