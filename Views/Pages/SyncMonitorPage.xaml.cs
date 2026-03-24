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
        private CancellationTokenSource? _syncCts;

        public SyncMonitorPage(
            AppDbContext dbContext,
            TallySyncOrchestrator orchestrator,
            SyncStateMonitor syncMonitor,
            TallyXmlService tallyService)
        {
            InitializeComponent();
            _dbContext = dbContext;
            _orchestrator = orchestrator;
            _syncMonitor = syncMonitor;
            _tallyService = tallyService;

            this.DataContext = _syncMonitor;
            
            _syncMonitor.PropertyChanged += _syncMonitor_PropertyChanged;
            Loaded += SyncMonitorPage_Loaded;
            Unloaded += SyncMonitorPage_Unloaded;
        }

        private void _syncMonitor_PropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            if (e.PropertyName == nameof(SyncStateMonitor.IsSyncing)
                || e.PropertyName == nameof(SyncStateMonitor.CurrentStage)
                || e.PropertyName == nameof(SyncStateMonitor.ProgressPercent)
                || e.PropertyName == nameof(SyncStateMonitor.IsProgressIndeterminate))
            {
                Dispatcher.Invoke(UpdateSyncStateUi);
            }
            else if (e.PropertyName == nameof(SyncStateMonitor.TotalRecordsSynced))
            {
                Dispatcher.Invoke(() => TotalSyncedText.Text = _syncMonitor.TotalRecordsSynced.ToString());
            }
        }

        private async void SyncMonitorPage_Loaded(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
        }

        private void SyncMonitorPage_Unloaded(object sender, RoutedEventArgs e)
        {
            _syncCts?.Cancel();
        }

        private async void Refresh_Click(object sender, RoutedEventArgs e)
        {
            await RefreshDataAsync();
        }

        private async Task RefreshDataAsync()
        {
            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                
                // Refresh Tally connection status
                var status = await _tallyService.DetectTallyStatusAsync();
                
                if (status == TallyConnectionStatus.RunningWithCompany)
                {
                    TallyDot.Fill = new SolidColorBrush(Color.FromRgb(16, 185, 129));
                    TallyStatusText.Text = "Connected";
                }
                else if (status == TallyConnectionStatus.RunningNoCompany)
                {
                    TallyDot.Fill = Brushes.Orange;
                    TallyStatusText.Text = "No Company Open";
                }
                else
                {
                    TallyDot.Fill = new SolidColorBrush(Color.FromRgb(239, 68, 68));
                    TallyStatusText.Text = "Not Running";
                }

                // Get Metadata
                _syncMonitor.EntitySyncDetails.Clear();

                // 🛡 Safeguard: If DbContext is not configured (common in MongoDB-only mode), skip metadata refresh
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
                        EntityName = meta.EntityType,
                        LastSync = meta.LastSuccessfulSync?.DateTime,
                        RecordsSynced = meta.RecordsSyncedInLastRun,
                        SyncStatus = meta.IsSyncRunning ? "RUNNING" : (string.IsNullOrEmpty(meta.LastError) ? "SUCCESS" : "ERROR")
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

        private async void SyncNow_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing) return;

            _syncCts = new CancellationTokenSource();
            _syncMonitor.AddLog("Manual sync triggered by user.", "INFO", "USER");

            try
            {
                var orgId = SessionManager.Instance.OrganizationId;
                await _orchestrator.RunFullSyncAsync(orgId, null, null, _syncCts.Token);
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
        }

        private void UpdateSyncStateUi()
        {
            SyncStateText.Text = _syncMonitor.IsSyncing
                ? _syncMonitor.CurrentStage
                : (_syncMonitor.CurrentStage == "Idle" ? "Idle" : _syncMonitor.CurrentStage);
            SyncProgress.Visibility = _syncMonitor.IsSyncing ? Visibility.Visible : Visibility.Collapsed;
            SyncButton.IsEnabled = !_syncMonitor.IsSyncing;
        }

        private async void DryRun_Click(object sender, RoutedEventArgs e)
        {
            if (_syncMonitor.IsSyncing) return;

            _syncCts = new CancellationTokenSource();
            _syncMonitor.AddLog("🚀 Starting 2-Day Dry Run Validation...", "INFO", "DRY-RUN");

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
