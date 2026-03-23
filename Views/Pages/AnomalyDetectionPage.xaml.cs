using System;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Analytics;
using Acczite20.Models.Analytics;

namespace Acczite20.Views.Pages
{
    public partial class AnomalyDetectionPage : Page
    {
        private readonly IFinancialAnalysisService _analysisService;

        public AnomalyDetectionPage(IFinancialAnalysisService analysisService)
        {
            InitializeComponent();
            _analysisService = analysisService;
            Loaded += AnomalyDetectionPage_Loaded;
        }

        private async void AnomalyDetectionPage_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var anomalies = await _analysisService.GetAnomaliesAsync(orgId);
                AnomalyList.ItemsSource = anomalies;
                
                if (anomalies.Count == 0)
                {
                    // Optionally show "No anomalies found"
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to scan for anomalies: {ex.Message}");
            }
        }

        private void Investigate_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.DataContext is AnomalyRecord record)
            {
                if (record.RelatedVoucherId.HasValue)
                {
                    // Navigate to Voucher Details
                }
            }
        }
    }
}
