using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Services;
using Acczite20.Services.Navigation;
using Acczite20.Services.Sync;
using Microsoft.Extensions.DependencyInjection;
using Acczite20.Views;

namespace Acczite20.Views.Pages
{
    public partial class TallyExecutePage : Page
    {
        private readonly List<string> _selectedTables;
        private readonly INavigationService _navigationService;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly TallySyncOrchestrator _orchestrator;
        private CancellationTokenSource? _syncCts;
        private bool _monitorHandlersAttached;

        public string Summary { get; set; }

        public TallyExecutePage(List<string> selectedTables, List<string> selectedFields, INavigationService navigationService)
        {
            _navigationService = navigationService;
            InitializeComponent();

            _selectedTables = selectedTables ?? new List<string>();

            var serviceProvider = ((App)Application.Current).ServiceProvider;
            _syncMonitor = serviceProvider.GetRequiredService<SyncStateMonitor>();
            _orchestrator = serviceProvider.GetRequiredService<TallySyncOrchestrator>();

            Summary = BuildSummary(selectedFields);

            DataContext = this;
            LiveLogList.ItemsSource = _syncMonitor.Logs;
            SyncProgressPanel.DataContext = _syncMonitor;

            Loaded += TallyExecutePage_Loaded;
            Unloaded += TallyExecutePage_Unloaded;
        }

        private string BuildSummary(List<string> selectedFields)
        {
            var tableSummary = _selectedTables.Count > 0
                ? $"Tables to sync:\n- {string.Join("\n- ", _selectedTables)}"
                : "Tables to sync:\n- Full organization dataset";

            var fieldSummary = selectedFields.Count > 0
                ? $"Tally fields:\n- {string.Join("\n- ", selectedFields)}"
                : "Tally fields:\n- All detected accounting collections";

            return $"{tableSummary}\n\n{fieldSummary}";
        }

        private void TallyExecutePage_Loaded(object sender, RoutedEventArgs e)
        {
            AttachMonitorHandlers();
            RefreshExecutionUi();

            // Default sync range: Inclusive start to catch current and previous fiscal years
            if (SyncFromDatePicker.SelectedDate == null)
                SyncFromDatePicker.SelectedDate = new DateTime(2024, 1, 1);
            if (SyncToDatePicker.SelectedDate == null)
                SyncToDatePicker.SelectedDate = DateTime.Today;
        }

        private void TallyExecutePage_Unloaded(object sender, RoutedEventArgs e)
        {
            DetachMonitorHandlers();
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack)
            {
                _navigationService.GoBack();
            }
        }

        private void AttachMonitorHandlers()
        {
            if (_monitorHandlersAttached)
            {
                return;
            }

            _syncMonitor.PropertyChanged += SyncMonitor_PropertyChanged;
            _monitorHandlersAttached = true;
        }

        private void DetachMonitorHandlers()
        {
            if (!_monitorHandlersAttached)
            {
                return;
            }

            _syncMonitor.PropertyChanged -= SyncMonitor_PropertyChanged;
            _monitorHandlersAttached = false;
        }

        private void SyncMonitor_PropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (e.PropertyName is nameof(SyncStateMonitor.IsSyncing)
                or nameof(SyncStateMonitor.CurrentStage)
                or nameof(SyncStateMonitor.CurrentStageDetail)
                or nameof(SyncStateMonitor.ProgressPercent)
                or nameof(SyncStateMonitor.IsProgressIndeterminate)
                or nameof(SyncStateMonitor.TotalRecordsSynced)
                or nameof(SyncStateMonitor.VouchersPerSecond)
                or nameof(SyncStateMonitor.MemoryUsage)
                or nameof(SyncStateMonitor.CurrentBatchVouchers))
            {
                Dispatcher.Invoke(RefreshExecutionUi);
            }
        }

        private void RefreshExecutionUi()
        {
            var hasVisibleRunState = _syncMonitor.IsSyncing
                || _syncMonitor.TotalRecordsSynced > 0
                || !string.Equals(_syncMonitor.CurrentStage, "Idle", StringComparison.OrdinalIgnoreCase);

            SyncProgressPanel.Visibility = hasVisibleRunState ? Visibility.Visible : Visibility.Collapsed;
            SyncRunRing.IsActive = _syncMonitor.IsSyncing;
            SyncRunRing.Visibility = _syncMonitor.IsSyncing ? Visibility.Visible : Visibility.Collapsed;

            SyncStatusText.Text = _syncMonitor.CurrentStage;
            SyncStageDetailText.Text = _syncMonitor.CurrentStageDetail;
            SyncProgressBar.IsIndeterminate = _syncMonitor.IsSyncing && _syncMonitor.IsProgressIndeterminate;
            SyncProgressBar.Value = _syncMonitor.IsProgressIndeterminate ? 0 : _syncMonitor.ProgressPercent;
            SyncProgressCaptionText.Text = _syncMonitor.ProgressCaption;
            SyncCheckpointText.Text = _syncMonitor.CheckpointStatus;
            SyncedRecordsText.Text = _syncMonitor.TotalRecordsSynced.ToString("N0");
            VoucherRateText.Text = $"{_syncMonitor.VouchersPerSecond:N0} /sec";
            MemoryUsageText.Text = _syncMonitor.MemoryUsage;
            BatchSnapshotText.Text = _syncMonitor.LastBatchSummary;

            SyncButton.IsEnabled = !_syncMonitor.IsSyncing;
            SyncButton.Content = _syncMonitor.IsSyncing
                ? "Synchronization Running..."
                : "Initialize Synchronization";

            ApplyRunStateBadge();
        }

        private void ApplyRunStateBadge()
        {
            string label;
            string backgroundHex;
            string foregroundHex;

            if (_syncMonitor.IsSyncing)
            {
                label = "Running";
                backgroundHex = "#DBEAFE";
                foregroundHex = "#1D4ED8";
            }
            else
            {
                switch (_syncMonitor.CurrentStage)
                {
                    case "Sync complete":
                        label = "Complete";
                        backgroundHex = "#DCFCE7";
                        foregroundHex = "#166534";
                        break;
                    case "Sync failed":
                        label = "Failed";
                        backgroundHex = "#FEE2E2";
                        foregroundHex = "#B91C1C";
                        break;
                    case "Sync cancelled":
                        label = "Cancelled";
                        backgroundHex = "#FEF3C7";
                        foregroundHex = "#92400E";
                        break;
                    default:
                        label = "Idle";
                        backgroundHex = "#E2E8F0";
                        foregroundHex = "#475569";
                        break;
                }
            }

            RunStateBadge.Background = BrushFromHex(backgroundHex);
            RunStateBadgeText.Foreground = BrushFromHex(foregroundHex);
            RunStateBadgeText.Text = label;
        }

        private static SolidColorBrush BrushFromHex(string hex)
        {
            return new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        }

        private async void Sync_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing)
            {
                SyncProgressPanel.Visibility = Visibility.Visible;
                RefreshExecutionUi();

                MessageBox.Show(
                    "A sync is already running. Live progress is shown below.",
                    "Sync In Progress",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            try
            {
                bool isMongo = string.Equals(SessionManager.Instance.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase);
                if (SessionManager.Instance.OrganizationId == Guid.Empty && (!isMongo || string.IsNullOrWhiteSpace(SessionManager.Instance.OrganizationObjectId)))
                {
                    MessageBox.Show(
                        "Please select a valid organization before starting a synchronization.",
                        "Organization Required",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning);
                    return;
                }

                // Sync parameters binding
                _syncMonitor.SyncMode = SyncModeCombo.SelectedIndex == 1 ? "Safe" : "Auto";
                if (_syncMonitor.SyncMode == "Safe")
                {
                    var item = (ComboBoxItem)BatchSizeCombo.SelectedItem;
                    if (item != null && int.TryParse(item.Content.ToString(), out int b))
                        _syncMonitor.BatchSize = b;
                    else
                        _syncMonitor.BatchSize = 25;
                }
                else _syncMonitor.BatchSize = 150;

                DateTimeOffset? fromDate = SyncFromDatePicker.SelectedDate;
                DateTimeOffset? toDate = SyncToDatePicker.SelectedDate;

                SyncProgressPanel.Visibility = Visibility.Visible;
                SyncButton.IsEnabled = false;
                _syncCts = new CancellationTokenSource();

                RefreshExecutionUi();
                await _orchestrator.RunFullSyncAsync(SessionManager.Instance.OrganizationId, fromDate, toDate, _syncCts.Token);
                RefreshExecutionUi();

                var dialog = new TallySyncCompleteDialog
                {
                    Owner = Window.GetWindow(this)
                };
                dialog.SetResults(_syncMonitor.TotalRecordsSynced, _syncMonitor.VouchersPerSecond);
                await dialog.ShowAsync();
            }
            catch (OperationCanceledException)
            {
                RefreshExecutionUi();
                MessageBox.Show(
                    "Synchronization was cancelled before completion.",
                    "Sync Cancelled",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
            }
            catch (Exception ex)
            {
                RefreshExecutionUi();
                var isExpectedSyncFailure = string.Equals(_syncMonitor.CurrentStage, "Sync failed", StringComparison.OrdinalIgnoreCase)
                    || ex is InvalidOperationException;
                var messagePrefix = isExpectedSyncFailure
                    ? "Synchronization failed.\n\n"
                    : "An unexpected error occurred while running the sync.\n\n";

                MessageBox.Show(
                    messagePrefix + ex.Message,
                    isExpectedSyncFailure ? "Synchronization Failed" : "Execution Error",
                    MessageBoxButton.OK,
                    isExpectedSyncFailure ? MessageBoxImage.Warning : MessageBoxImage.Error);
            }
            finally
            {
                SyncButton.IsEnabled = !_syncMonitor.IsSyncing;
                _syncCts?.Dispose();
                _syncCts = null;
            }
        }

        private void SyncModeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (SyncModeCombo == null || BatchSizePanel == null) return;
            BatchSizePanel.Visibility = SyncModeCombo.SelectedIndex == 1 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void PauseButton_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor == null) return;
            _syncMonitor.IsPaused = !_syncMonitor.IsPaused;
            PauseButton.Content = _syncMonitor.IsPaused ? "Resume" : "Pause";
        }

        private void ForceCooldown_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor == null) return;
            _syncMonitor.TriggerCooldown = true;
        }
    }
}

