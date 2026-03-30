using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public enum SyncStatus
    {
        Idle,
        Running,
        Success,
        Failed,
        Cancelled
    }

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
        private static readonly Process _currentProcess = Process.GetCurrentProcess();

        public event PropertyChangedEventHandler? PropertyChanged;

        public ObservableCollection<SyncLogViewModel> Logs { get; } = new ObservableCollection<SyncLogViewModel>();
        public ObservableCollection<EntitySyncStatus> EntitySyncDetails { get; } = new ObservableCollection<EntitySyncStatus>();
        public ObservableCollection<FailedSyncRecord> DeadLetterQueue { get; } = new ObservableCollection<FailedSyncRecord>();

        private CancellationTokenSource? _metricsCts;
        private DateTime _runStartTime;

        private SyncStatus _status = SyncStatus.Idle;
        public SyncStatus Status
        {
            get => _status;
            set { _status = value; OnPropertyChanged(); OnPropertyChanged(nameof(IsSyncing)); }
        }

        public bool IsSyncing => Status == SyncStatus.Running;

        private string _failureReason = string.Empty;
        public string FailureReason
        {
            get => _failureReason;
            set { _failureReason = value; OnPropertyChanged(); }
        }

        // --- Granular Counters ---
        private int _fetchedCount;
        public int FetchedCount
        {
            get => _fetchedCount;
            set { _fetchedCount = value; OnPropertyChanged(); }
        }

        private int _savedCount;
        public int SavedCount
        {
            get => _savedCount;
            set { _savedCount = value; OnPropertyChanged(); }
        }

        private int _skippedCount;
        public int SkippedCount
        {
            get => _skippedCount;
            set { _skippedCount = value; OnPropertyChanged(); }
        }

        private int _totalErrors;
        public int TotalErrors
        {
            get => _totalErrors;
            set { _totalErrors = value; OnPropertyChanged(); }
        }

        // --- Connection Stats ---
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

        private DateTime? _lastBackgroundSync;
        public DateTime? LastBackgroundSync
        {
            get => _lastBackgroundSync;
            set { _lastBackgroundSync = value; OnPropertyChanged(); }
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

        private string _syncMode = "Auto";
        public string SyncMode
        {
            get => _syncMode;
            set { _syncMode = value; OnPropertyChanged(); OnPropertyChanged(nameof(LiveMetricsSummary)); }
        }

        private int _currentBatchVouchers;
        public int CurrentBatchVouchers
        {
            get => _currentBatchVouchers;
            set { _currentBatchVouchers = value; OnPropertyChanged(); }
        }

        private int _batchSize = 150;
        public int BatchSize
        {
            get => _batchSize;
            set { _batchSize = value; OnPropertyChanged(); }
        }

        private int _interBatchDelayMs = 2000;
        public int InterBatchDelayMs
        {
            get => _interBatchDelayMs;
            set { _interBatchDelayMs = value; OnPropertyChanged(); }
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

        private bool _isProgressIndeterminate;
        public bool IsProgressIndeterminate
        {
            get => _isProgressIndeterminate;
            set { _isProgressIndeterminate = value; OnPropertyChanged(); }
        }

        private string _currentStage = "Idle";
        public string CurrentStage
        {
            get => _currentStage;
            set { _currentStage = value; OnPropertyChanged(); }
        }

        private string _currentStageDetail = "Waiting for initialization.";
        public string CurrentStageDetail
        {
            get => _currentStageDetail;
            set { _currentStageDetail = value; OnPropertyChanged(); }
        }

        public string ProgressCaption => Status == SyncStatus.Running
            ? $"{Math.Round(ProgressPercent):0}% of the current sync pipeline"
            : (Status == SyncStatus.Success ? "Sync successful" : "Sync engine idle");

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

        // --- Backwards Compatibility / Legacy Props ---
        private string _tallyHealth = "Stable";
        public string TallyHealth
        {
            get => _tallyHealth;
            set { _tallyHealth = value; OnPropertyChanged(); }
        }

        private double _liveWindowHours;
        public double LiveWindowHours
        {
            get => _liveWindowHours;
            set { _liveWindowHours = value; OnPropertyChanged(); }
        }

        private int _liveDelayMs;
        public int LiveDelayMs
        {
            get => _liveDelayMs;
            set { _liveDelayMs = value; OnPropertyChanged(); }
        }

        private int _liveRetries;
        public int LiveRetries
        {
            get => _liveRetries;
            set { _liveRetries = value; OnPropertyChanged(); }
        }

        private bool _isPaused;
        public bool IsPaused
        {
            get => _isPaused;
            set { _isPaused = value; OnPropertyChanged(); }
        }

        private DateTime? _lastSyncTime;
        public DateTime? LastSyncTime
        {
            get => _lastSyncTime;
            set { _lastSyncTime = value; OnPropertyChanged(); }
        }

        private int _mongoQueueDepth;
        public int MongoQueueDepth
        {
            get => _mongoQueueDepth;
            set { _mongoQueueDepth = value; OnPropertyChanged(); }
        }

        private DateTime? _lastMongoProjectedAt;
        public DateTime? LastMongoProjectedAt
        {
            get => _lastMongoProjectedAt;
            set { _lastMongoProjectedAt = value; OnPropertyChanged(); }
        }

        private long _mongoProjectionLagMs;
        public long MongoProjectionLagMs
        {
            get => _mongoProjectionLagMs;
            set { _mongoProjectionLagMs = value; OnPropertyChanged(); }
        }

        private int _replayQueueSize;
        public int ReplayQueueSize
        {
            get => _replayQueueSize;
            set { _replayQueueSize = value; OnPropertyChanged(); }
        }

        public string CheckpointStatus => $"{TotalRecordsSynced:N0} records synced so far";
        public string LastBatchSummary => $"Current Run Summary: Fetched {FetchedCount:N0}, Saved {SavedCount:N0}";
        public string LiveMetricsSummary => $"Mode: {SyncMode} | Progress: {ProgressPercent:0}%";

        public void BeginRun(string stage, string detail)
        {
            Status = SyncStatus.Running;
            CurrentStage = stage;
            CurrentStageDetail = detail;
            FetchedCount = 0;
            SavedCount = 0;
            SkippedCount = 0;
            ProgressPercent = 0;
            IsProgressIndeterminate = true;
            FailureReason = string.Empty;
            TotalRecordsSynced = 0;
            _runStartTime = DateTime.Now;
            
            StartBackgroundMetrics();
        }

        private void StartBackgroundMetrics()
        {
            _metricsCts?.Cancel();
            _metricsCts = new CancellationTokenSource();
            var token = _metricsCts.Token;

            Task.Run(async () =>
            {
                try
                {
                    while (!token.IsCancellationRequested && IsSyncing)
                    {
                        MemoryUsage = GetCurrentMemoryUsage();
                        LastBackgroundSync = DateTime.Now;
                        await Task.Delay(2000, token);
                    }
                }
                catch (OperationCanceledException) { }
            }, token);
        }

        private string GetCurrentMemoryUsage()
        {
            try
            {
                _currentProcess.Refresh();
                return $"{_currentProcess.PrivateMemorySize64 / 1024 / 1024} MB";
            }
            catch { return "0 MB"; }
        }

        public void CompleteRun(string detail)
        {
            Status = SyncStatus.Success;
            ProgressPercent = 100;
            IsProgressIndeterminate = false;
            CurrentStage = "Sync complete";
            CurrentStageDetail = detail;
            _lastSyncTime = DateTime.Now;
            _metricsCts?.Cancel();
        }

        public void FailRun(string reason)
        {
            Status = SyncStatus.Failed;
            FailureReason = reason;
            IsProgressIndeterminate = false;
            CurrentStage = "Sync failed";
            CurrentStageDetail = reason;
            _metricsCts?.Cancel();
        }

        public void CancelRun(string detail)
        {
            Status = SyncStatus.Cancelled;
            CurrentStage = "Sync cancelled";
            CurrentStageDetail = detail;
            _metricsCts?.Cancel();
        }

        public void SetStage(string stage, string detail, double progress, bool isIndeterminate = false)
        {
            CurrentStage = stage;
            CurrentStageDetail = detail;
            ProgressPercent = progress;
            IsProgressIndeterminate = isIndeterminate;
        }

        public void AddLog(string message, string level = "INFO", string module = "SYSTEM")
        {
            var app = System.Windows.Application.Current;
            if (app == null) return;

            app.Dispatcher.BeginInvoke(() =>
            {
                try
                {
                    Logs.Add(new SyncLogViewModel { Message = message, Level = level, Module = module });
                    if (Logs.Count > 1000) Logs.RemoveAt(0);
                }
                catch { }
            });
        }

        public void Reset()
        {
            System.Windows.Application.Current?.Dispatcher?.Invoke(() =>
            {
                Logs.Clear();
                Status = SyncStatus.Idle;
                TotalRecordsSynced = 0;
                FetchedCount = 0;
                SavedCount = 0;
                SkippedCount = 0;
                ProgressPercent = 0;
                FailureReason = string.Empty;
            });
        }

        public void RecordInsertedBatch(int vCount, int lCount, int iCount, int bCount, double elapsedSeconds)
        {
            SavedCount += vCount;
            TotalRecordsSynced = SavedCount;
            
            if (elapsedSeconds > 0)
            {
                VouchersPerSecond = SavedCount / elapsedSeconds;
            }

            OnPropertyChanged(nameof(CheckpointStatus));
            OnPropertyChanged(nameof(LastBatchSummary));
        }

        public void UpdateMetrics(int totalSynced, double elapsedSeconds)
        {
            TotalRecordsSynced = totalSynced;
            if (elapsedSeconds > 0)
            {
                VouchersPerSecond = totalSynced / elapsedSeconds;
            }
        }

        public void IncrementSkipped(int count = 1)
        {
            SkippedCount += count;
        }

        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
