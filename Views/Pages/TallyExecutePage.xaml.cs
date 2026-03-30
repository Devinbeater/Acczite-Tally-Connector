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
        private readonly List<string> _selectedTallyCollections;
        private readonly INavigationService _navigationService;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly TallySyncOrchestrator _orchestrator;
        private readonly ISyncControlService _control;
        private CancellationTokenSource? _syncCts;
        private bool _monitorHandlersAttached;

        public string Summary { get; set; }

        public TallyExecutePage(List<string> selectedTables, List<string> selectedFields, INavigationService navigationService)
        {
            _navigationService = navigationService;
            InitializeComponent();

            _selectedTables = selectedTables ?? new List<string>();
            _selectedTallyCollections = selectedFields ?? new List<string>();

            var serviceProvider = ((App)Application.Current).ServiceProvider;
            _syncMonitor = serviceProvider.GetRequiredService<SyncStateMonitor>();
            _orchestrator = serviceProvider.GetRequiredService<TallySyncOrchestrator>();
            _control = serviceProvider.GetRequiredService<ISyncControlService>();

            Summary = BuildSummary(selectedFields);

            DataContext = this;
            LiveLogGrid.ItemsSource = _syncMonitor.Logs;

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
            // Failure Banner logic
            FailureBanner.Visibility = _syncMonitor.Status == SyncStatus.Failed ? Visibility.Visible : Visibility.Collapsed;
            FailureReasonText.Text = _syncMonitor.FailureReason;

            SyncStatusLabel.Text = _syncMonitor.Status == SyncStatus.Running ? "RUNNING" : _syncMonitor.Status.ToString().ToUpper();
            SyncStatusLabel.Foreground = _syncMonitor.Status switch
            {
                SyncStatus.Running => (SolidColorBrush)Application.Current.Resources["PrimaryTeal"],
                SyncStatus.Success => Brushes.MediumSeaGreen,
                SyncStatus.Failed => Brushes.IndianRed,
                _ => (SolidColorBrush)Application.Current.Resources["GrayText"]
            };

            CurrentStepText.Text = _syncMonitor.CurrentStage;
            CurrentStepDetailText.Text = _syncMonitor.CurrentStageDetail;
            
            SyncProgressBar.IsIndeterminate = _syncMonitor.IsSyncing && _syncMonitor.IsProgressIndeterminate;
            SyncProgressBar.Value = _syncMonitor.IsProgressIndeterminate ? 0 : _syncMonitor.ProgressPercent;
            SyncProgressCaptionText.Text = _syncMonitor.ProgressCaption;
            
            FetchedCountText.Text = _syncMonitor.FetchedCount.ToString("N0");
            SavedCountText.Text = _syncMonitor.SavedCount.ToString("N0");
            SkippedCountText.Text = _syncMonitor.SkippedCount.ToString();
            VoucherRateText.Text = $"{_syncMonitor.VouchersPerSecond} /sec";
            MemoryUsageText.Text = _syncMonitor.MemoryUsage;
            LastSyncTimeText.Text = _syncMonitor.LastBackgroundSync?.ToString("HH:mm:ss") ?? "Never";

            SyncButton.IsEnabled = !_syncMonitor.IsSyncing;
            SyncButtonText.Text = _syncMonitor.IsSyncing ? "Sync Running..." : "Start Sync";

            // Auto-scroll the log grid (Chronological: scroll to bottom)
            if (LiveLogGrid.Items.Count > 0)
            {
                var border = VisualTreeHelper.GetChild(LiveLogGrid, 0) as Border;
                if (border != null)
                {
                    var scrollViewer = VisualTreeHelper.GetChild(border, 0) as ScrollViewer;
                    scrollViewer?.ScrollToEnd(); 
                }
            }
        }

        private static SolidColorBrush BrushFromHex(string hex)
        {
            return new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        }

        private async void Sync_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing)
            {
                RefreshExecutionUi();

                await CustomDialog.ShowAsync(
                    "Sync In Progress",
                    "A synchronization is already running. Live progress is shown below.",
                    CustomDialog.DialogType.Info);
                return;
            }

            var orgId = SessionManager.Instance.OrganizationId;
            try
            {
                if (orgId == Guid.Empty && string.IsNullOrWhiteSpace(SessionManager.Instance.OrganizationObjectId))
                {
                    await CustomDialog.ShowAsync(
                        "Organization Required",
                        "Please select a valid organization before starting a synchronization.",
                        CustomDialog.DialogType.Warning);
                    return;
                }

                var runId = Guid.NewGuid();

                // Sync parameters binding
                _syncMonitor.SyncMode = SyncModeCombo.SelectedIndex == 2 ? "Full" : (SyncModeCombo.SelectedIndex == 1 ? "Delta" : "Auto");
                _syncMonitor.BatchSize = _syncMonitor.SyncMode == "SafeMode" ? 25 : 150;

                DateTimeOffset? fromDate = SyncFromDatePicker.SelectedDate;
                DateTimeOffset? toDate = SyncToDatePicker.SelectedDate;

                SyncButton.IsEnabled = false;
                _syncCts = new CancellationTokenSource();

                RefreshExecutionUi();
                var result = await _orchestrator.RunFullSyncAsync(orgId, fromDate, toDate, _syncCts.Token, _selectedTallyCollections, runId: runId);
                RefreshExecutionUi();

                if (result == SyncRunResult.Ignored)
                {
                    var status = _control.GetState(orgId);
                    if (status.Owner == SyncOwner.HostedService)
                    {
                        await CustomDialog.ShowAsync(
                            "Background Sync Active",
                            "Background sync is currently processing. Please wait for it to finish or stop it from the dashboard.",
                            CustomDialog.DialogType.Warning);
                    }
                    else
                    {
                        await CustomDialog.ShowAsync(
                            "Sync Already Active",
                            "A synchronization cycle is already running for this organization. You can monitor progress in the log view.",
                            CustomDialog.DialogType.Info);
                    }
                    return;
                }

                if (result == SyncRunResult.Failed || string.Equals(_syncMonitor.CurrentStage, "Sync failed", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(_syncMonitor.CurrentStageDetail ?? "Tally connection failed or sync error.");
                }
                
                if (result == SyncRunResult.Cancelled || string.Equals(_syncMonitor.CurrentStage, "Sync cancelled", StringComparison.OrdinalIgnoreCase))
                {
                    throw new OperationCanceledException();
                }

                // Only show dialog if something actually happened
                if (_syncMonitor.FetchedCount > 0 || _syncMonitor.SavedCount > 0 || _syncMonitor.TotalRecordsSynced > 0)
                {
                    var dialog = new TallySyncCompleteDialog
                    {
                        Owner = Window.GetWindow(this)
                    };
                    dialog.SetResults(_syncMonitor.TotalRecordsSynced, _syncMonitor.VouchersPerSecond);
                    await dialog.ShowAsync();
                }
                else 
                {
                    await CustomDialog.ShowAsync("Sync Finished", "Tally reported no new records to sync for this range.", CustomDialog.DialogType.Info);
                }
            }
            catch (OperationCanceledException)
            {
                RefreshExecutionUi();
                await CustomDialog.ShowAsync(
                    "Sync Cancelled",
                    "Synchronization was cancelled before completion.",
                    CustomDialog.DialogType.Info);
            }
            catch (Exception ex)
            {
                RefreshExecutionUi();
                var isExpectedSyncFailure = string.Equals(_syncMonitor.CurrentStage, "Sync failed", StringComparison.OrdinalIgnoreCase)
                    || ex is InvalidOperationException;
                var messagePrefix = isExpectedSyncFailure
                    ? "Synchronization failed.\n\n"
                    : "An unexpected error occurred while running the sync.\n\n";

                await CustomDialog.ShowAsync(
                    isExpectedSyncFailure ? "Synchronization Failed" : "Execution Error",
                    messagePrefix + ex.Message,
                    isExpectedSyncFailure ? CustomDialog.DialogType.Warning : CustomDialog.DialogType.Error);
            }
            finally
            {
                // Orchestrator handles state cleanup (Idle/Completed/Failed) via ISyncControlService.Complete
                // We just need to restore UI button state here
                _syncMonitor.LastBackgroundSync = DateTime.Now;
                
                SyncButton.IsEnabled = !_syncMonitor.IsSyncing;
                _syncCts = null;
                RefreshExecutionUi();
            }
        }

        private async void ExpandLogs_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new FullLogViewerDialog(_syncMonitor.Logs)
            {
                Owner = System.Windows.Window.GetWindow(this)
            };
            await dialog.ShowAsync();
        }

        private void ContinuousToggle_Toggled(object sender, RoutedEventArgs e)
        {
            if (ContinuousToggle == null) return;
            
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty) return;

            var state = _control.GetState(orgId);
            state.IsContinuous = ContinuousToggle.IsOn;
            
            _syncMonitor.AddLog($"Continuous Sync switched to {(state.IsContinuous ? "ENABLED" : "DISABLED")}", "INFO", "CONFIG");
        }
    }
}

