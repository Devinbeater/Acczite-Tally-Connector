using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Acczite20.Data;
using Acczite20.Models;
using Acczite20.Services;
using Acczite20.Services.Sync;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Acczite20.Views.Pages
{
    public partial class SyncMonitorPage : Page
    {
        private readonly AppDbContext _dbContext;
        private readonly TallySyncOrchestrator _orchestrator;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly TallyXmlService _tallyService;
        private readonly ISyncControlService _control;
        private CancellationTokenSource? _syncCts;

        public SyncMonitorPage(
            AppDbContext dbContext,
            TallySyncOrchestrator orchestrator,
            SyncStateMonitor syncMonitor,
            TallyXmlService tallyService,
            ISyncControlService control)
        {
            InitializeComponent();
            _dbContext    = dbContext;
            _orchestrator = orchestrator;
            _syncMonitor  = syncMonitor;
            _tallyService = tallyService;
            _control      = control;

            this.DataContext = _syncMonitor;

            _syncMonitor.PropertyChanged += SyncMonitor_PropertyChanged;
            Loaded   += SyncMonitorPage_Loaded;
            Unloaded += SyncMonitorPage_Unloaded;
        }

        private void SyncMonitor_PropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            switch (e.PropertyName)
            {
                case nameof(SyncStateMonitor.IsSyncing):
                case nameof(SyncStateMonitor.CurrentStage):
                case nameof(SyncStateMonitor.ProgressPercent):
                case nameof(SyncStateMonitor.IsProgressIndeterminate):
                    Dispatcher.Invoke(UpdateSyncStateUi);
                    break;

                case nameof(SyncStateMonitor.TotalRecordsSynced):
                    Dispatcher.Invoke(() => { /* bound */ });
                    break;

                case nameof(SyncStateMonitor.TallyHealth):
                    Dispatcher.Invoke(UpdateHealthPill);
                    break;

                case nameof(SyncStateMonitor.LiveWindowHours):
                    Dispatcher.Invoke(() =>
                        LiveWindowText.Text = _syncMonitor.LiveWindowHours > 0
                            ? $"{_syncMonitor.LiveWindowHours:0.#}h"
                            : "—");
                    break;

                case nameof(SyncStateMonitor.LiveDelayMs):
                    Dispatcher.Invoke(() =>
                        LiveDelayText.Text = _syncMonitor.LiveDelayMs > 0
                            ? $"{_syncMonitor.LiveDelayMs / 1000.0:0.#}s"
                            : "0s");
                    break;

                case nameof(SyncStateMonitor.LiveRetries):
                    Dispatcher.Invoke(() =>
                        LiveRetriesText.Text = _syncMonitor.LiveRetries.ToString());
                    break;
            }
        }

        private async void SyncMonitorPage_Loaded(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
            // Initialise delay panel visibility based on default mode.
            UpdateDelayPanelVisibility();
        }

        private void SyncMonitorPage_Unloaded(object sender, RoutedEventArgs e)
        {
            _syncCts?.Cancel();
        }

        // ── Control event handlers ────────────────────────────────────────────────

        private void SyncModeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (SyncModeCombo.SelectedItem is ComboBoxItem item)
            {
                _syncMonitor.SyncMode = item.Tag?.ToString() ?? "Auto";
                UpdateDelayPanelVisibility();
            }
        }

        private void BatchSizeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (BatchSizeCombo.SelectedItem is ComboBoxItem item &&
                int.TryParse(item.Tag?.ToString(), out var size))
            {
                _syncMonitor.BatchSize = size;
            }
        }

        private void BatchDelayCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (BatchDelayCombo.SelectedItem is ComboBoxItem item &&
                int.TryParse(item.Tag?.ToString(), out var ms))
            {
                _syncMonitor.InterBatchDelayMs = ms;
            }
        }

        private void UpdateDelayPanelVisibility()
        {
            if (DelayPanel == null) return;
            DelayPanel.Visibility = _syncMonitor.SyncMode == "Safe"
                ? Visibility.Visible
                : Visibility.Collapsed;
        }

        // ── Pause / Resume ────────────────────────────────────────────────────────

        private async void PauseResume_Click(object sender, RoutedEventArgs e)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            var state = _control.GetState(orgId);

            if (state.IsPaused)
            {
                _control.Resume(orgId);
                PauseResumeButton.Content = "Pause";
            }
            else
            {
                await _control.PauseAsync(orgId);
                PauseResumeButton.Content = "Resume";
            }
        }

        // ── Sync actions ──────────────────────────────────────────────────────────

        private async void SyncNow_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing) return;

            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty)
            {
                await CustomDialog.ShowAsync("Organization Required", "Please select a valid organization.", CustomDialog.DialogType.Warning);
                return;
            }

            var runId = Guid.NewGuid();

            // Apply selected batch size to the monitor so the orchestrator picks it up.
            if (BatchSizeCombo.SelectedItem is ComboBoxItem batchItem &&
                int.TryParse(batchItem.Tag?.ToString(), out var bs))
                _syncMonitor.BatchSize = bs;

            if (SyncModeCombo.SelectedItem is ComboBoxItem modeItem)
                _syncMonitor.SyncMode = modeItem.Tag?.ToString() ?? "Auto";

            if (BatchDelayCombo.SelectedItem is ComboBoxItem delayItem &&
                int.TryParse(delayItem.Tag?.ToString(), out var delayMs))
                _syncMonitor.InterBatchDelayMs = delayMs;

            _syncMonitor.IsPaused = false;
            PauseResumeButton.Content  = "Pause";
            PauseResumeButton.IsEnabled = true;
            _syncMonitor.LiveWindowHours = 0;
            _syncMonitor.LiveDelayMs     = 0;
            _syncMonitor.LiveRetries     = 0;
            LiveWindowText.Text  = "—";
            LiveDelayText.Text   = "—";
            LiveRetriesText.Text = "0";

            _syncCts = new CancellationTokenSource();
            _syncMonitor.AddLog(
                $"Manual sync triggered — Mode: {_syncMonitor.SyncMode} | Batch: {_syncMonitor.BatchSize} | Delay: {_syncMonitor.InterBatchDelayMs / 1000.0:0.#}s",
                "INFO", "USER");

            try
            {
                var result = await _orchestrator.RunFullSyncAsync(orgId, null, null, _syncCts.Token, runId: runId);
                
                if (result == SyncRunResult.Ignored)
                {
                    await CustomDialog.ShowAsync("Sync Already Active", "A synchronization is already running for this organization.", CustomDialog.DialogType.Info);
                    return;
                }

                await RefreshDataAsync();
            }
            catch (OperationCanceledException)
            {
                _syncMonitor.AddLog("Sync cancelled.", "WARNING", "ORCHESTRATOR");
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"Manual sync failed: {ex.Message}", "ERROR", "ORCHESTRATOR");
            }
            finally
            {
                // Orchestrator handles state cleanup (Idle/Completed/Failed)
                PauseResumeButton.IsEnabled = false;
                _syncMonitor.IsPaused = false;
                PauseResumeButton.Content = "Pause";
                UpdateSyncStateUi();
            }
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
        }

        // ── Data refresh ──────────────────────────────────────────────────────────

        private async Task RefreshDataAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;

                var status = await _tallyService.DetectTallyStatusAsync();

                if (status == TallyConnectionStatus.RunningWithCompany)
                {
                    TallyDot.Fill       = new SolidColorBrush(Color.FromRgb(16, 185, 129));
                    TallyStatusText.Text = "Connected";
                }
                else if (status == TallyConnectionStatus.RunningNoCompany)
                {
                    TallyDot.Fill       = Brushes.Orange;
                    TallyStatusText.Text = "No Company Open";
                }
                else
                {
                    TallyDot.Fill       = new SolidColorBrush(Color.FromRgb(239, 68, 68));
                    TallyStatusText.Text = "Not Running";
                }

                UpdateHealthPill();

                if (string.IsNullOrEmpty(_dbContext.Database.ProviderName))
                {
                    _syncMonitor.AddLog("SQL Metadata skipped (No DB Provider configured).", "INFO", "MONITOR");
                    UpdateSyncStateUi();
                    return;
                }

                var metadata = await _dbContext.SyncMetadataRecords
                    .OrderByDescending(m => m.LastSuccessfulSync)
                    .ToListAsync();

                _syncMonitor.EntitySyncDetails.Clear();
                int total = 0;
                DateTimeOffset? latest = null;

                foreach (var meta in metadata)
                {
                    _syncMonitor.EntitySyncDetails.Add(new EntitySyncStatus
                    {
                        EntityName  = meta.EntityType,
                        LastSync    = meta.LastSuccessfulSync?.DateTime,
                        RecordsSynced = meta.RecordsSyncedInLastRun,
                        SyncStatus  = meta.IsSyncRunning ? "RUNNING" : (string.IsNullOrEmpty(meta.LastError) ? "SUCCESS" : "ERROR")
                    });
                    total += meta.RecordsSyncedInLastRun;
                    if (latest == null || meta.LastSuccessfulSync > latest) latest = meta.LastSuccessfulSync;
                }

                _syncMonitor.TotalRecordsSynced = total;
                _syncMonitor.LastSyncTime = latest?.DateTime;
                LastSyncText.Text = _syncMonitor.LastSyncTime?.ToString("dd MMM HH:mm") ?? "Never";

                UpdateSyncStateUi();
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"Failed to refresh monitor: {ex.Message}", "ERROR", "MONITOR");
            }
        }

        // ── UI helpers ────────────────────────────────────────────────────────────

        private void UpdateSyncStateUi()
        {
            SyncStateText.Text = _syncMonitor.IsSyncing
                ? _syncMonitor.CurrentStage
                : _syncMonitor.CurrentStage;
            SyncProgress.Visibility = _syncMonitor.IsSyncing ? Visibility.Visible : Visibility.Collapsed;
            SyncButton.IsEnabled    = !_syncMonitor.IsSyncing;
            SyncButton.Content      = _syncMonitor.IsSyncing ? "Syncing…" : "Start Sync";
            PauseResumeButton.IsEnabled = _syncMonitor.IsSyncing;
            if (!_syncMonitor.IsSyncing)
            {
                _syncMonitor.IsPaused     = false;
                PauseResumeButton.Content = "Pause";
            }
        }

        private void UpdateHealthPill()
        {
            var health = _syncMonitor.TallyHealth;
            TallyHealthSubtext.Text = health;

            var (bg, fg) = health switch
            {
                "Stable"     => ("#10B981", "White"),
                "Slow"       => ("#F59E0B", "White"),
                "Overloaded" => ("#EF4444", "White"),
                _            => ("#6B7280", "White")
            };

            TallyHealthSubtext.Foreground = new SolidColorBrush(
                (Color)ColorConverter.ConvertFromString(bg));

            HealthPill.Background = new SolidColorBrush(
                (Color)ColorConverter.ConvertFromString(bg));
            HealthText.Text = health;
        }

        private async void DryRun_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing) return;

            _syncCts = new CancellationTokenSource();
            _syncMonitor.AddLog("Starting 2-Day Dry Run Validation...", "INFO", "DRY-RUN");

            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                await _orchestrator.RunDryRunValidationAsync(orgId, _syncCts.Token);
                await RefreshDataAsync();
            }
            catch (OperationCanceledException)
            {
                _syncMonitor.AddLog("Dry run cancelled.", "WARNING", "DRY-RUN");
            }
            catch (Exception ex)
            {
                _syncMonitor.AddLog($"Dry run failed: {ex.Message}", "ERROR", "DRY-RUN");
            }
        }
    }
}
