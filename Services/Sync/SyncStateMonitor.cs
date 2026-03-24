using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Acczite20.Services.Sync
{
    public class SyncLogViewModel
    {
        public DateTime Timestamp { get; set; } = DateTime.Now;
        public string Message { get; set; } = string.Empty;
        public string Level { get; set; } = "INFO";
        public string Module { get; set; } = "SYSTEM";
        public string FullLogLine => $"[{Timestamp:HH:mm:ss}] {Level}: {Message}";
    }

    public class FailedSyncRecord
    {
        public string TallyMasterId { get; set; } = string.Empty;
        public string Reference { get; set; } = string.Empty;
        public string Error { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; } = DateTime.Now;
    }

    public class EntitySyncStatus
    {
        // Frozen brush allocated once for the SUCCESS state (avoids per-access allocation)
        private static readonly System.Windows.Media.SolidColorBrush SuccessBrush;

        static EntitySyncStatus()
        {
            SuccessBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(16, 185, 129));
            SuccessBrush.Freeze();
        }

        public string EntityName { get; set; } = string.Empty;
        public DateTime? LastSync { get; set; }
        public int RecordsSynced { get; set; }
        public string SyncStatus { get; set; } = "IDLE";

        // Helpers for UI
        public string StatusText => SyncStatus;
        public System.Windows.Media.Brush StatusColor => SyncStatus switch
        {
            "RUNNING" => System.Windows.Media.Brushes.DeepSkyBlue,
            "SUCCESS" => SuccessBrush,
            "ERROR"   => System.Windows.Media.Brushes.Red,
            _         => System.Windows.Media.Brushes.Gray
        };
    }

    public class SyncStateMonitor : INotifyPropertyChanged
    {
        private const int CheckpointSize = 100;
        private static readonly Process _currentProcess = Process.GetCurrentProcess();

        public event PropertyChangedEventHandler? PropertyChanged;

        public ObservableCollection<SyncLogViewModel> Logs { get; } = new ObservableCollection<SyncLogViewModel>();
        public ObservableCollection<EntitySyncStatus> EntitySyncDetails { get; } = new ObservableCollection<EntitySyncStatus>();
        public ObservableCollection<FailedSyncRecord> DeadLetterQueue { get; } = new ObservableCollection<FailedSyncRecord>();

        private int _totalErrors;
        public int TotalErrors
        {
            get => _totalErrors;
            set { _totalErrors = value; OnPropertyChanged(); }
        }

        private bool _isMongoConnected;
        public bool IsMongoConnected
        {
            get => _isMongoConnected;
            set { _isMongoConnected = value; OnPropertyChanged(); }
        }

        private bool _isTallyConnected;
        public bool IsTallyConnected
        {
            get => _isTallyConnected;
            set { _isTallyConnected = value; OnPropertyChanged(); }
        }

        private int _totalRecordsSynced;
        public int TotalRecordsSynced
        {
            get => _totalRecordsSynced;
            set
            {
                _totalRecordsSynced = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(CheckpointStatus));
                OnPropertyChanged(nameof(LastBatchSummary));
            }
        }

        private bool _isSyncing;
        public bool IsSyncing
        {
            get => _isSyncing;
            set
            {
                _isSyncing = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ProgressCaption));
                OnPropertyChanged(nameof(LiveMetricsSummary));
            }
        }

        private bool _isPaused;
        public bool IsPaused
        {
            get => _isPaused;
            set { _isPaused = value; OnPropertyChanged(); }
        }

        private string _syncMode = "Auto";
        public string SyncMode
        {
            get => _syncMode;
            set { _syncMode = value; OnPropertyChanged(); OnPropertyChanged(nameof(LiveMetricsSummary)); }
        }

        private int _batchSize = 150; // Default matches standard cap
        public int BatchSize
        {
            get => _batchSize;
            set { _batchSize = value; OnPropertyChanged(); }
        }

        private string _tallyHealth = "Stable";
        public string TallyHealth
        {
            get => _tallyHealth;
            set
            {
                if (_tallyHealth == value) return;
                _tallyHealth = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(TallyHealthColor));
                OnPropertyChanged(nameof(TallyHealthDot));
            }
        }

        /// <summary>UI hex color matching Tally health state.</summary>
        public string TallyHealthColor => TallyHealth switch
        {
            "Stable"     => "#10B981",
            "Slow"       => "#F59E0B",
            "Overloaded" => "#EF4444",
            _            => "#6B7280"
        };

        /// <summary>Dot fill brush for the Tally health indicator.</summary>
        public System.Windows.Media.SolidColorBrush TallyHealthDot
        {
            get
            {
                var hex = TallyHealthColor;
                var r = Convert.ToByte(hex.Substring(1, 2), 16);
                var g = Convert.ToByte(hex.Substring(3, 2), 16);
                var b = Convert.ToByte(hex.Substring(5, 2), 16);
                var brush = new System.Windows.Media.SolidColorBrush(
                    System.Windows.Media.Color.FromRgb(r, g, b));
                brush.Freeze();
                return brush;
            }
        }

        private bool _triggerCooldown;
        public bool TriggerCooldown
        {
            get => _triggerCooldown;
            set { _triggerCooldown = value; OnPropertyChanged(); }
        }

        // ── Live runtime metrics (updated each chunk by VoucherSyncController) ──

        private int _interBatchDelayMs;
        public int InterBatchDelayMs
        {
            get => _interBatchDelayMs;
            set { _interBatchDelayMs = value; OnPropertyChanged(); }
        }

        private double _liveWindowHours;
        public double LiveWindowHours
        {
            get => _liveWindowHours;
            set { _liveWindowHours = value; OnPropertyChanged(); OnPropertyChanged(nameof(LiveMetricsSummary)); }
        }

        private int _liveDelayMs;
        public int LiveDelayMs
        {
            get => _liveDelayMs;
            set { _liveDelayMs = value; OnPropertyChanged(); OnPropertyChanged(nameof(LiveMetricsSummary)); }
        }

        private int _liveRetries;
        public int LiveRetries
        {
            get => _liveRetries;
            set { _liveRetries = value; OnPropertyChanged(); OnPropertyChanged(nameof(LiveMetricsSummary)); }
        }

        /// <summary>One-line metrics strip shown in the UI header area.</summary>
        public string LiveMetricsSummary => IsSyncing
            ? $"Window {LiveWindowHours:0.#}h | Delay {LiveDelayMs / 1000.0:0.#}s | Retries {LiveRetries} | Mode {SyncMode}"
            : "Sync engine idle";

        private DateTime? _lastSyncTime;
        public DateTime? LastSyncTime
        {
            get => _lastSyncTime;
            set { _lastSyncTime = value; OnPropertyChanged(); }
        }

        private string _memoryUsage = "0 MB";
        public string MemoryUsage
        {
            get => _memoryUsage;
            set { _memoryUsage = value; OnPropertyChanged(); }
        }

        private double _vouchersPerSecond;
        public double VouchersPerSecond
        {
            get => _vouchersPerSecond;
            set { _vouchersPerSecond = value; OnPropertyChanged(); }
        }

        private string _currentStage = "Idle";
        public string CurrentStage
        {
            get => _currentStage;
            set { _currentStage = value; OnPropertyChanged(); }
        }

        private string _currentStageDetail = "Waiting for the next sync cycle.";
        public string CurrentStageDetail
        {
            get => _currentStageDetail;
            set { _currentStageDetail = value; OnPropertyChanged(); }
        }

        private double _progressPercent;
        public double ProgressPercent
        {
            get => _progressPercent;
            set
            {
                _progressPercent = Math.Max(0, Math.Min(100, value));
                OnPropertyChanged();
                OnPropertyChanged(nameof(ProgressCaption));
            }
        }

        private int _currentBatchVouchers;
        public int CurrentBatchVouchers
        {
            get => _currentBatchVouchers;
            set
            {
                _currentBatchVouchers = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(LastBatchSummary));
            }
        }

        private int _currentBatchLedgerEntries;
        public int CurrentBatchLedgerEntries
        {
            get => _currentBatchLedgerEntries;
            set
            {
                _currentBatchLedgerEntries = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(LastBatchSummary));
            }
        }

        private int _currentBatchInventoryEntries;
        public int CurrentBatchInventoryEntries
        {
            get => _currentBatchInventoryEntries;
            set
            {
                _currentBatchInventoryEntries = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(LastBatchSummary));
            }
        }

        private bool _isProgressIndeterminate;
        public bool IsProgressIndeterminate
        {
            get => _isProgressIndeterminate;
            set { _isProgressIndeterminate = value; OnPropertyChanged(); }
        }

        public string ProgressCaption => IsSyncing
            ? $"{Math.Round(ProgressPercent):0}% of the current sync pipeline"
            : "Sync engine idle";

        public string CheckpointStatus
        {
            get
            {
                if (TotalRecordsSynced <= 0)
                {
                    return $"Next checkpoint at {CheckpointSize:N0} synced records";
                }

                var nextCheckpoint = ((TotalRecordsSynced / CheckpointSize) + 1) * CheckpointSize;
                return $"{TotalRecordsSynced:N0} synced so far | next checkpoint {nextCheckpoint:N0}";
            }
        }

        public string LastBatchSummary => CurrentBatchVouchers <= 0
            ? "Waiting for the first insert batch"
            : $"Last batch {CurrentBatchVouchers:N0} vouchers | {CurrentBatchLedgerEntries:N0} ledgers | {CurrentBatchInventoryEntries:N0} inventory rows";

        public void RecordError(string masterId, string reference, string error)
        {
            TotalErrors++;
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                DeadLetterQueue.Add(new FailedSyncRecord 
                { 
                    TallyMasterId = masterId, 
                    Reference = reference, 
                    Error = error 
                });
                
                if (DeadLetterQueue.Count > 1000) DeadLetterQueue.RemoveAt(0); // Cap size
            });
            
            AddLog($"Voucher {reference} failed: {error}", "ERROR", "SYNC");
        }

        public void BeginRun(string title, string description)
        {
            TotalRecordsSynced = 0;
            CurrentBatchVouchers = 0;
            CurrentBatchLedgerEntries = 0;
            CurrentBatchInventoryEntries = 0;
            VouchersPerSecond = 0;
            MemoryUsage = "0 MB";
            CurrentStage = title;
            CurrentStageDetail = description;
            ProgressPercent = 0;
            IsProgressIndeterminate = true;
            IsSyncing = true;
        }

        public void SetStage(string stage, string detail, double progressPercent, bool isIndeterminate = false)
        {
            CurrentStage = stage;
            CurrentStageDetail = detail;
            ProgressPercent = progressPercent;
            IsProgressIndeterminate = isIndeterminate;
        }

        public void RecordInsertedBatch(int vouchers, int ledgerEntries, int inventoryEntries, double elapsedSeconds)
        {
            CurrentBatchVouchers = vouchers;
            CurrentBatchLedgerEntries = ledgerEntries;
            CurrentBatchInventoryEntries = inventoryEntries;
            TotalRecordsSynced += vouchers;
            UpdateMetrics(TotalRecordsSynced, elapsedSeconds);

            var stageProgress = vouchers > 0
                ? Math.Min(94, 58 + (Math.Log10(TotalRecordsSynced + 1) * 16))
                : ProgressPercent;

            SetStage(
                "Syncing vouchers",
                $"Committed {TotalRecordsSynced:N0} vouchers to the database.",
                stageProgress,
                false);
        }

        public void CompleteRun(string detail)
        {
            IsProgressIndeterminate = false;
            ProgressPercent = 100;
            CurrentStage = "Sync complete";
            CurrentStageDetail = detail;
            LastSyncTime = DateTime.Now;
            IsSyncing = false;
        }

        public void FailRun(string detail)
        {
            IsProgressIndeterminate = false;
            ProgressPercent = Math.Max(ProgressPercent, 15);
            CurrentStage = "Sync failed";
            CurrentStageDetail = detail;
            IsSyncing = false;
        }

        public void CancelRun(string detail)
        {
            IsProgressIndeterminate = false;
            CurrentStage = "Sync cancelled";
            CurrentStageDetail = detail;
            IsSyncing = false;
        }

        public void UpdateMetrics(int totalSynced, double elapsedSeconds)
        {
            if (elapsedSeconds > 0)
                VouchersPerSecond = Math.Round(totalSynced / elapsedSeconds, 0);
            
            _currentProcess.Refresh(); // Refresh cached OS counters before reading
            MemoryUsage = $"{_currentProcess.PrivateMemorySize64 / 1024 / 1024} MB";
        }

        public void AddLog(string message, string level = "INFO", string module = "SYSTEM")
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() =>
            {
                Logs.Insert(0, new SyncLogViewModel { Message = message, Level = level, Module = module });
                if (Logs.Count > 100) Logs.RemoveAt(100);
            });
        }

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}

