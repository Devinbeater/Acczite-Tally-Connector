using System;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services;
using Acczite20.Services.Analytics;
using Acczite20.Models.Analytics;
using System.Threading.Tasks;
using System.Diagnostics;
using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public partial class ExecutiveDashboardPage : Page
    {
        private readonly IBusinessPulseService _pulseService;
        private readonly IReportingService _reportingService;
        private readonly INavigationService _navigationService;
        private BusinessPulseStats? _lastStats;

        public ExecutiveDashboardPage(IBusinessPulseService pulseService, IReportingService reportingService, INavigationService navigationService)
        {
            InitializeComponent();
            _pulseService = pulseService;
            _reportingService = reportingService;
            _navigationService = navigationService;
            Loaded += ExecutiveDashboardPage_Loaded;
            
            TxtClock.Text = DateTime.Now.ToString("dddd, dd MMMM yyyy • hh:mm tt");

            _timer = new System.Windows.Threading.DispatcherTimer();
            _timer.Interval = TimeSpan.FromSeconds(30);
            _timer.Tick += async (s, e) => {
                TxtClock.Text = DateTime.Now.ToString("dddd, dd MMMM yyyy • hh:mm tt");
                await RefreshDataAsync();
            };
            _timer.Start();

            Unloaded += (s, e) => _timer.Stop();
        }

        private readonly System.Windows.Threading.DispatcherTimer _timer;

        private async void ExecutiveDashboardPage_Loaded(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
        }

        private async void Download_Click(object sender, RoutedEventArgs e)
        {
            if (_lastStats == null) return;
            
            try
            {
                var path = await _reportingService.ExportWeeklyReportAsync(_lastStats);
                Process.Start(new ProcessStartInfo(path) { UseShellExecute = true });
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Export failed: {ex.Message}");
            }
        }

        private async Task RefreshDataAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                _lastStats = await _pulseService.GetDailyPulseAsync(orgId);

                TxtSalesToday.Text = $"₹ {_lastStats.SalesToday:N2}";
                TxtInvoicesToday.Text = $"{_lastStats.InvoicesToday} Invoices";
                TxtCollectionsToday.Text = $"₹ {_lastStats.CollectionsToday:N2}";
                TxtCashPosition.Text = $"₹ {_lastStats.NetCashBank:N2}";
                TxtReceivables.Text = $"₹ {_lastStats.TotalReceivables:N2}";
                TxtPayables.Text = $"₹ {_lastStats.TotalPayables:N2}";
                
                AlertsList.ItemsSource = _lastStats.Alerts;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to load Business Pulse: {ex.Message}");
            }
        }
        private void ViewTimeline_Click(object sender, RoutedEventArgs e)
        {
            _navigationService.NavigateTo<TimelinePage>("Unified Global Timeline");
        }
    }
}
