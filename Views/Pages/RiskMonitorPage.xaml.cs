using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Analytics;
using Acczite20.Models.Analytics;

namespace Acczite20.Views.Pages
{
    public partial class RiskMonitorPage : Page
    {
        private readonly ICustomerRiskService _riskService;

        public RiskMonitorPage(ICustomerRiskService riskService)
        {
            InitializeComponent();
            _riskService = riskService;
            Loaded += RiskMonitorPage_Loaded;
        }

        private async void RiskMonitorPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadRiskDataAsync();
        }

        private async Task LoadRiskDataAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                var risks = await _riskService.GetHighRiskCustomersAsync(orgId);

                RiskList.ItemsSource = risks;

                TxtHighRiskCount.Text = risks.Count(r => r.RiskLevel == "High" || r.RiskLevel == "Critical").ToString();
                TxtTotalExposure.Text = $"₹ {risks.Sum(r => r.TotalOutstanding):N2}";
                
                // Static for now until paging/behavior logic is more complex
                TxtAvgDelay.Text = "42 Days"; 
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load risk analysis: {ex.Message}");
            }
        }
    }
}
