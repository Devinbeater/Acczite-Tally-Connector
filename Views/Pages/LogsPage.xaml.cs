using System;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using Acczite20.Services.Sync;
using Acczite20.Services.Navigation;

namespace Acczite20.Views.Pages
{
    public partial class LogsPage : Page
    {
        private readonly SyncStateMonitor _monitor;
        private readonly INavigationService _navigationService;

        public SyncStateMonitor Monitor => _monitor;

        public LogsPage(SyncStateMonitor monitor, INavigationService navigationService)
        {
            _monitor = monitor;
            _navigationService = navigationService;
            InitializeComponent();
            DataContext = this;
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack) _navigationService.GoBack();
        }
    }

    public class LogEntry
    {
        public string Timestamp { get; set; } = string.Empty;
        public string Level { get; set; } = string.Empty;
        public string Module { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }
}
