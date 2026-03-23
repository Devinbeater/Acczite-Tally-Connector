using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Reports;
using LiveChartsCore;
using LiveChartsCore.SkiaSharpView;
using LiveChartsCore.SkiaSharpView.VisualElements;
using SkiaSharp;

namespace Acczite20.Views.Pages
{
    public partial class GstReportingPage : Page
    {
        private readonly GstReportService _gstService;

        public GstReportingPage(GstReportService gstService)
        {
            InitializeComponent();
            _gstService = gstService;

            FromDatePicker.SelectedDate = DateTime.Today.AddMonths(-1);
            ToDatePicker.SelectedDate = DateTime.Today;

            Loaded += GstReportingPage_Loaded;
        }

        private async void GstReportingPage_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadReportAsync();
        }

        private async void Generate_Click(object sender, RoutedEventArgs e)
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

                var summary = await _gstService.GetGstSummaryAsync(orgId, fromDate, toDate);
                var rateWise = await _gstService.GetRateWiseSummaryAsync(orgId, fromDate, toDate);

                // Update UI
                TxtTaxable.Text = $"₹ {summary.TotalTaxableValue:N2}";
                TxtCgst.Text = $"₹ {summary.TotalCgst:N2}";
                TxtSgst.Text = $"₹ {summary.TotalSgst:N2}";
                TxtIgst.Text = $"₹ {summary.TotalIgst:N2}";

                GstGrid.ItemsSource = rateWise;

                // Update Chart
                UpdateChart(summary);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load GST report: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void UpdateChart(GstSummaryModel summary)
        {
            GstPieChart.Series = new ISeries[]
            {
                new PieSeries<decimal> { Values = new[] { summary.TotalCgst }, Name = "CGST", Pushout = 4 },
                new PieSeries<decimal> { Values = new[] { summary.TotalSgst }, Name = "SGST", Pushout = 4 },
                new PieSeries<decimal> { Values = new[] { summary.TotalIgst }, Name = "IGST", Pushout = 4 }
            };
        }

        private void ExportExcel_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Export to Excel feature is coming soon in the next release!", "Coming Soon", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        private void ExportPdf_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Export to PDF feature is coming soon in the next release!", "Coming Soon", MessageBoxButton.OK, MessageBoxImage.Information);
        }
    }
}
