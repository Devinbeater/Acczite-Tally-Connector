using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using Acczite20.Models;
using Acczite20.Services;
using Acczite20.Services.Dashboard;
using Acczite20.Services.Navigation;
using Acczite20.Services.Sync;
using Acczite20.Services.Tally;
using LiveChartsCore;
using LiveChartsCore.SkiaSharpView;
using LiveChartsCore.SkiaSharpView.Painting;
using SkiaSharp;

namespace Acczite20.Views.Pages
{
    [SupportedOSPlatform("windows7.0")]
    public partial class DashboardPage : Page, INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler? PropertyChanged;

        private ISeries[] _series = Array.Empty<ISeries>();
        private Axis[] _xAxes = Array.Empty<Axis>();
        private Axis[] _yAxes = Array.Empty<Axis>();
        private ISeries[] _gstSeries = Array.Empty<ISeries>();
        private ISeries[] _inventoryChartSeries = Array.Empty<ISeries>();
        private Axis[] _inventoryChartXAxes = Array.Empty<Axis>();
        private Axis[] _inventoryChartYAxes = Array.Empty<Axis>();
        private ISeries[] _waveSeries = Array.Empty<ISeries>();
        private Axis[] _waveXAxes = Array.Empty<Axis>();
        private Axis[] _waveYAxes = Array.Empty<Axis>();
        private string[] _timelineHeaders = Array.Empty<string>();
        private string _connectedCompanyName = "-";

        private readonly TallyXmlService _tallyService;
        private readonly TallyCompanyService _tallyCompanyService;
        private readonly SyncStateMonitor _syncMonitor;
        private readonly INavigationService _navigationService;
        private readonly DashboardService _dashboardService;
        private readonly ISyncControlService _syncControl;
        private readonly DispatcherTimer _refreshTimer;

        public ISeries[] Series
        {
            get => _series;
            set
            {
                _series = value;
                OnPropertyChanged();
            }
        }

        public Axis[] XAxes
        {
            get => _xAxes;
            set
            {
                _xAxes = value;
                OnPropertyChanged();
            }
        }

        public Axis[] YAxes
        {
            get => _yAxes;
            set
            {
                _yAxes = value;
                OnPropertyChanged();
            }
        }

        public ISeries[] GstSeries
        {
            get => _gstSeries;
            set
            {
                _gstSeries = value;
                OnPropertyChanged();
            }
        }

        public ISeries[] InventoryChartSeries
        {
            get => _inventoryChartSeries;
            set
            {
                _inventoryChartSeries = value;
                OnPropertyChanged();
            }
        }

        public Axis[] InventoryChartXAxes
        {
            get => _inventoryChartXAxes;
            set
            {
                _inventoryChartXAxes = value;
                OnPropertyChanged();
            }
        }

        public Axis[] InventoryChartYAxes
        {
            get => _inventoryChartYAxes;
            set
            {
                _inventoryChartYAxes = value;
                OnPropertyChanged();
            }
        }

        public ISeries[] WaveSeries
        {
            get => _waveSeries;
            set
            {
                _waveSeries = value;
                OnPropertyChanged();
            }
        }

        public Axis[] WaveXAxes
        {
            get => _waveXAxes;
            set
            {
                _waveXAxes = value;
                OnPropertyChanged();
            }
        }

        public Axis[] WaveYAxes
        {
            get => _waveYAxes;
            set
            {
                _waveYAxes = value;
                OnPropertyChanged();
            }
        }

        public string[] TimelineHeaders
        {
            get => _timelineHeaders;
            set
            {
                _timelineHeaders = value;
                OnPropertyChanged();
            }
        }

        public ObservableCollection<DashboardFunnelStage> FunnelStages { get; } = new();

        public ObservableCollection<DashboardTimelineStage> TimelineStages { get; } = new();

        public string ConnectedCompanyName
        {
            get => _connectedCompanyName;
            set
            {
                _connectedCompanyName = value;
                OnPropertyChanged();
            }
        }

        public SyncState ControlState => _syncControl.GetState(SessionManager.Instance.OrganizationId);

        public SyncStateMonitor Monitor => _syncMonitor;

        public DashboardPage(
            SyncStateMonitor syncMonitor,
            INavigationService navigationService,
            DashboardService dashboardService,
            TallyXmlService tallyService,
            TallyCompanyService tallyCompanyService,
            ISyncControlService syncControl)
        {
            _syncMonitor = syncMonitor;
            _navigationService = navigationService;
            _dashboardService = dashboardService;
            _tallyService = tallyService;
            _tallyCompanyService = tallyCompanyService;
            _syncControl = syncControl;

            InitializeComponent();
            DataContext = this;
            Loaded += DashboardPage_Loaded;
            Unloaded += DashboardPage_Unloaded;

            _refreshTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(2)
            };
            _refreshTimer.Tick += async (_, _) => 
            {
                OnPropertyChanged(nameof(ControlState));
                await LoadDashboardDataAsync();
            };
        }

        protected void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            if (propertyName is null)
            {
                return;
            }

            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }

        private void DashboardPage_Unloaded(object sender, RoutedEventArgs e)
        {
            _refreshTimer.Stop();
        }

        private void Back_Click(object sender, RoutedEventArgs e)
        {
            if (_navigationService.CanGoBack)
            {
                _navigationService.GoBack();
            }
        }

        private async void DashboardPage_Loaded(object sender, RoutedEventArgs e)
        {
            OrgLabel.Text = string.IsNullOrWhiteSpace(SessionManager.Instance.OrganizationName)
                ? "Current Organization"
                : SessionManager.Instance.OrganizationName;

            LoadChart(new DashboardStats());

            await LoadDashboardDataAsync();
            _refreshTimer.Start();

            await RefreshTallyStatusAsync(addLog: false);
            LastSyncTime.Text = DateTime.Now.ToString("hh:mm tt");
        }

        private async void CheckNow_Click(object sender, RoutedEventArgs e)
        {
            SyncStatusLabel.Text = "Checking...";
            SyncDot.Fill = CreateBrush("#F59E0B");
            ConnectedCompanyName = "Checking...";

            await RefreshTallyStatusAsync(addLog: true);
            LastSyncTime.Text = DateTime.Now.ToString("hh:mm tt");
        }

        private void StopSync_Click(object sender, RoutedEventArgs e)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            _syncControl.CancelSync(orgId);
            OnPropertyChanged(nameof(ControlState));
        }

        private async void PauseSync_Click(object sender, RoutedEventArgs e)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            await _syncControl.PauseAsync(orgId);
            OnPropertyChanged(nameof(ControlState));
        }

        private void ResumeSync_Click(object sender, RoutedEventArgs e)
        {
            var orgId = SessionManager.Instance.OrganizationId;
            _syncControl.Resume(orgId);
            OnPropertyChanged(nameof(ControlState));
        }

        private async System.Threading.Tasks.Task RefreshTallyStatusAsync(bool addLog)
        {
            TallyStatusLabel.Text = "Checking...";
            var status = await _tallyService.DetectTallyStatusAsync();
            string? companyName = null;

            if (status == TallyConnectionStatus.RunningWithCompany)
            {
                companyName = await _tallyCompanyService.GetOpenCompanyAsync();
            }

            switch (status)
            {
                case TallyConnectionStatus.RunningWithCompany:
                    TallyStatusLabel.Text = "Running";
                    TallyStatusLabel.Foreground = CreateBrush("#22C55E");
                    SyncDot.Fill = CreateBrush("#22C55E");
                    SyncStatusLabel.Text = "Sync Engine Ready";
                    ConnectedCompanyName = string.IsNullOrWhiteSpace(companyName) ? "Connected" : companyName;
                    if (addLog)
                    {
                        AddLog("INFO", "Tally connection verified");
                    }

                    break;
                case TallyConnectionStatus.RunningNoCompany:
                    TallyStatusLabel.Text = "No Company";
                    TallyStatusLabel.Foreground = CreateBrush("#F59E0B");
                    SyncDot.Fill = CreateBrush("#F59E0B");
                    SyncStatusLabel.Text = "Tally Running - Open a Company";
                    ConnectedCompanyName = "No Company Open";
                    if (addLog)
                    {
                        AddLog("WARN", "Tally has no company open");
                    }

                    break;
                case TallyConnectionStatus.NotRunning:
                    TallyStatusLabel.Text = "Offline";
                    TallyStatusLabel.Foreground = CreateBrush("#EF4444");
                    SyncDot.Fill = CreateBrush("#EF4444");
                    SyncStatusLabel.Text = "Tally Not Detected";
                    ConnectedCompanyName = "-";
                    if (addLog)
                    {
                        AddLog("ERROR", "Tally connection failed");
                    }

                    break;
            }
        }

        private void AddLog(string level, string message)
        {
            _syncMonitor.AddLog(message, level, "DASHBOARD");
        }

        private async System.Threading.Tasks.Task LoadDashboardDataAsync()
        {
            var orgId = SessionManager.Instance.OrganizationId;
            if (orgId == Guid.Empty && string.IsNullOrEmpty(SessionManager.Instance.OrganizationObjectId))
            {
                return;
            }

            try
            {
                var stats = await _dashboardService.GetStatsAsync(orgId);

                KpiVouchers.Text = stats.TotalVouchers.ToString("N0");
                KpiLedgers.Text = stats.TotalLedgers.ToString("N0");
                KpiStockItems.Text = stats.StockItemCount.ToString("N0");
                KpiSyncedToday.Text = stats.SyncedToday.ToString("N0");

                LoadChart(stats);
            }
            catch (Exception ex)
            {
                AddLog("ERROR", $"Failed to load dashboard data: {ex.Message}");
            }
        }

        private void LoadChart(DashboardStats stats)
        {
            var volumes = GetVolumeSeries(stats);
            var gstData = GetGstSeries(stats);
            var inventorySource = GetInventorySource(stats);

            TimelineHeaders = volumes.Select(v => v.Date.ToString("MMM dd")).ToArray();

            BuildVoucherLineChart(volumes);
            BuildWaveChart(volumes);
            BuildGstPieChart(gstData);
            BuildInventoryBarChart(inventorySource);
            BuildFunnelChart(inventorySource);
            BuildTimelineChart(stats, gstData, inventorySource);
        }

        private void BuildVoucherLineChart(IReadOnlyList<DailyVoucherVolume> volumes)
        {
            var labelsPaint = new SolidColorPaint(SKColor.Parse("#6B7280"));
            var separatorPaint = new SolidColorPaint(SKColor.Parse("#E5E7EB")) { StrokeThickness = 1 };

            Series = new ISeries[]
            {
                new LineSeries<double>
                {
                    Values = volumes.Select(v => (double)v.Count).ToArray(),
                    Name = "Vouchers",
                    Stroke = new SolidColorPaint(SKColor.Parse("#0F766E")) { StrokeThickness = 3 },
                    Fill = null,
                    GeometrySize = 8,
                    GeometryStroke = new SolidColorPaint(SKColor.Parse("#0F766E")) { StrokeThickness = 2 }
                }
            };

            XAxes = new[]
            {
                new Axis
                {
                    Labels = volumes.Select(v => v.Date.ToString("MMM dd")).ToArray(),
                    LabelsPaint = labelsPaint,
                    SeparatorsPaint = separatorPaint
                }
            };

            YAxes = new[]
            {
                new Axis
                {
                    LabelsPaint = labelsPaint,
                    SeparatorsPaint = separatorPaint,
                    MinLimit = 0
                }
            };
        }

        private void BuildWaveChart(IReadOnlyList<DailyVoucherVolume> volumes)
        {
            var values = volumes.Select(v => (double)v.Count).ToArray();
            var movingAverage = BuildMovingAverage(values);
            var labelsPaint = new SolidColorPaint(SKColor.Parse("#6B7280"));
            var separatorPaint = new SolidColorPaint(SKColor.Parse("#E5E7EB")) { StrokeThickness = 1 };

            WaveSeries = new ISeries[]
            {
                new LineSeries<double>
                {
                    Values = values,
                    Name = "Daily Flow",
                    Stroke = new SolidColorPaint(SKColor.Parse("#06B6D4")) { StrokeThickness = 4 },
                    Fill = new SolidColorPaint(SKColor.Parse("#06B6D4").WithAlpha(55)),
                    GeometrySize = 0,
                    LineSmoothness = 0.9
                },
                new LineSeries<double>
                {
                    Values = movingAverage,
                    Name = "Rolling Mean",
                    Stroke = new SolidColorPaint(SKColor.Parse("#0F766E")) { StrokeThickness = 2 },
                    Fill = null,
                    GeometrySize = 7,
                    GeometryStroke = new SolidColorPaint(SKColor.Parse("#0F766E")) { StrokeThickness = 2 },
                    LineSmoothness = 0.9
                }
            };

            WaveXAxes = new[]
            {
                new Axis
                {
                    Labels = volumes.Select(v => v.Date.ToString("MMM dd")).ToArray(),
                    LabelsPaint = labelsPaint,
                    SeparatorsPaint = separatorPaint
                }
            };

            WaveYAxes = new[]
            {
                new Axis
                {
                    LabelsPaint = labelsPaint,
                    SeparatorsPaint = separatorPaint,
                    MinLimit = 0
                }
            };
        }

        private void BuildGstPieChart(IReadOnlyList<GstDistribution> gstData)
        {
            var series = new List<ISeries>();

            foreach (var gst in gstData.Where(x => x.TotalAmount > 0))
            {
                var color = gst.TaxType.Contains("CGST", StringComparison.OrdinalIgnoreCase)
                    ? SKColor.Parse("#3B82F6")
                    : gst.TaxType.Contains("SGST", StringComparison.OrdinalIgnoreCase)
                        ? SKColor.Parse("#10B981")
                        : gst.TaxType.Contains("IGST", StringComparison.OrdinalIgnoreCase)
                            ? SKColor.Parse("#F59E0B")
                            : SKColor.Parse("#94A3B8");

                series.Add(new PieSeries<double>
                {
                    Values = new[] { (double)gst.TotalAmount },
                    Name = gst.TaxType,
                    Fill = new SolidColorPaint(color),
                    Pushout = 4
                });
            }

            if (series.Count == 0)
            {
                series.Add(new PieSeries<double>
                {
                    Values = new[] { 1d },
                    Name = "No GST data",
                    Fill = new SolidColorPaint(SKColor.Parse("#CBD5E1"))
                });
            }

            GstSeries = series.ToArray();
        }

        private void BuildInventoryBarChart(IReadOnlyList<InventoryInsight> inventorySource)
        {
            var labelsPaint = new SolidColorPaint(SKColor.Parse("#6B7280"));
            var separatorPaint = new SolidColorPaint(SKColor.Parse("#E5E7EB")) { StrokeThickness = 1 };

            InventoryChartSeries = new ISeries[]
            {
                new ColumnSeries<double>
                {
                    Values = inventorySource.Select(i => (double)i.QuantitySold).ToArray(),
                    Name = "Qty Sold",
                    Fill = new SolidColorPaint(SKColor.Parse("#0F766E")),
                    Stroke = new SolidColorPaint(SKColor.Parse("#0B5D56")) { StrokeThickness = 1 },
                    MaxBarWidth = 42
                }
            };

            InventoryChartXAxes = new[]
            {
                new Axis
                {
                    Labels = inventorySource.Select(i => TrimLabel(i.ItemName, 10)).ToArray(),
                    LabelsPaint = labelsPaint,
                    LabelsRotation = 0,
                    SeparatorsPaint = null
                }
            };

            InventoryChartYAxes = new[]
            {
                new Axis
                {
                    LabelsPaint = labelsPaint,
                    SeparatorsPaint = separatorPaint,
                    MinLimit = 0,
                    Labeler = value => value.ToString("N0")
                }
            };
        }

        private void BuildFunnelChart(IReadOnlyList<InventoryInsight> inventorySource)
        {
            FunnelStages.Clear();

            var maxValue = inventorySource.Max(i => (double)i.QuantitySold);

            for (var i = 0; i < inventorySource.Count; i++)
            {
                var item = inventorySource[i];
                var ratio = maxValue <= 0 ? 0 : (double)item.QuantitySold / maxValue;
                var width = maxValue <= 0 ? 320 - (i * 32) : 120 + (ratio * 220);

                FunnelStages.Add(new DashboardFunnelStage
                {
                    Stage = item.ItemName,
                    Value = item.QuantitySold,
                    Width = Math.Clamp(width, 120, 340),
                    Caption = maxValue <= 0
                        ? "Awaiting activity"
                        : i == 0
                            ? "Lead mover"
                            : $"{ratio:P0} of leader",
                    Fill = CreateBrush(GetPaletteColor(i))
                });
            }
        }

        private void BuildTimelineChart(
            DashboardStats stats,
            IReadOnlyList<GstDistribution> gstData,
            IReadOnlyList<InventoryInsight> inventorySource)
        {
            TimelineStages.Clear();

            var scaleBase = Math.Max(
                1d,
                new[]
                {
                    (double)stats.TotalVouchers,
                    stats.TotalLedgers,
                    stats.StockItemCount,
                    stats.SyncedToday
                }.Max());

            var gstLanes = Math.Max(1, gstData.Count(x => x.TotalAmount > 0));
            var voucherSpan = ScaleToSpan(stats.TotalVouchers, scaleBase, 3);
            var ledgerSpan = ScaleToSpan(stats.TotalLedgers, scaleBase, 2);
            var inventorySpan = ScaleToSpan(stats.StockItemCount, scaleBase, 3);
            var publishSpan = ScaleToSpan(Math.Max(stats.SyncedToday, 1), scaleBase, 2);

            TimelineStages.Add(new DashboardTimelineStage
            {
                Stage = "Connection Check",
                Detail = "Tally endpoint and company status",
                StartColumn = 0,
                Span = 1,
                DurationLabel = "1d",
                Fill = CreateBrush("#0F766E")
            });

            TimelineStages.Add(new DashboardTimelineStage
            {
                Stage = "Voucher Sweep",
                Detail = $"{stats.TotalVouchers:N0} vouchers scanned",
                StartColumn = 1,
                Span = ClampSpan(1, voucherSpan),
                DurationLabel = $"{ClampSpan(1, voucherSpan)}d",
                Fill = CreateBrush("#14B8A6")
            });

            TimelineStages.Add(new DashboardTimelineStage
            {
                Stage = "Ledger Mapping",
                Detail = $"{stats.TotalLedgers:N0} ledgers reconciled",
                StartColumn = 2,
                Span = ClampSpan(2, ledgerSpan),
                DurationLabel = $"{ClampSpan(2, ledgerSpan)}d",
                Fill = CreateBrush("#38BDF8")
            });

            TimelineStages.Add(new DashboardTimelineStage
            {
                Stage = "GST and Inventory",
                Detail = $"{gstLanes} GST lanes and {inventorySource.Count} top movers",
                StartColumn = 3,
                Span = ClampSpan(3, Math.Max(inventorySpan, 2)),
                DurationLabel = $"{ClampSpan(3, Math.Max(inventorySpan, 2))}d",
                Fill = CreateBrush("#F59E0B")
            });

            TimelineStages.Add(new DashboardTimelineStage
            {
                Stage = "Dashboard Publish",
                Detail = $"{stats.SyncedToday:N0} records landed today",
                StartColumn = 7 - publishSpan,
                Span = publishSpan,
                DurationLabel = $"{publishSpan}d",
                Fill = CreateBrush("#8B5CF6")
            });
        }

        private static List<DailyVoucherVolume> GetVolumeSeries(DashboardStats stats)
        {
            var volumes = stats.VoucherVolumes ?? new List<DailyVoucherVolume>();
            if (volumes.Count > 0)
            {
                return volumes
                    .OrderBy(v => v.Date)
                    .TakeLast(7)
                    .ToList();
            }

            return Enumerable.Range(0, 7)
                .Select(i => new DailyVoucherVolume
                {
                    Date = DateTime.Today.AddDays(-6 + i),
                    Count = 0
                })
                .ToList();
        }

        private static List<GstDistribution> GetGstSeries(DashboardStats stats)
        {
            return stats.GstDistributions?
                .OrderByDescending(g => g.TotalAmount)
                .ToList() ?? new List<GstDistribution>();
        }

        private static List<InventoryInsight> GetInventorySource(DashboardStats stats)
        {
            var items = stats.InventoryInsights?
                .Where(i => !string.IsNullOrWhiteSpace(i.ItemName))
                .OrderByDescending(i => i.QuantitySold)
                .Take(5)
                .ToList() ?? new List<InventoryInsight>();

            if (items.Count == 0)
            {
                return new List<InventoryInsight>
                {
                    new() { ItemName = "Item A", QuantitySold = 0 },
                    new() { ItemName = "Item B", QuantitySold = 0 },
                    new() { ItemName = "Item C", QuantitySold = 0 },
                    new() { ItemName = "Item D", QuantitySold = 0 },
                    new() { ItemName = "Item E", QuantitySold = 0 }
                };
            }

            return items;
        }

        private static double[] BuildMovingAverage(IReadOnlyList<double> values)
        {
            var result = new double[values.Count];

            for (var i = 0; i < values.Count; i++)
            {
                var start = Math.Max(0, i - 1);
                var end = Math.Min(values.Count - 1, i + 1);
                var count = end - start + 1;
                var total = 0d;

                for (var j = start; j <= end; j++)
                {
                    total += values[j];
                }

                result[i] = count == 0 ? 0 : total / count;
            }

            return result;
        }

        private static int ScaleToSpan(double value, double maxValue, int maxSpan)
        {
            if (value <= 0 || maxValue <= 0)
            {
                return 1;
            }

            return Math.Clamp((int)Math.Ceiling((value / maxValue) * maxSpan), 1, maxSpan);
        }

        private static int ClampSpan(int startColumn, int span)
        {
            return Math.Clamp(span, 1, 7 - startColumn);
        }

        private static string TrimLabel(string value, int maxLength)
        {
            if (string.IsNullOrWhiteSpace(value) || value.Length <= maxLength)
            {
                return value;
            }

            return $"{value[..(maxLength - 3)]}...";
        }

        private static string GetPaletteColor(int index)
        {
            return index switch
            {
                0 => "#0F766E",
                1 => "#14B8A6",
                2 => "#38BDF8",
                3 => "#F59E0B",
                _ => "#8B5CF6"
            };
        }

        private static Brush CreateBrush(string hex)
        {
            return new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        }
    }

    public sealed class DashboardFunnelStage
    {
        public string Stage { get; init; } = string.Empty;

        public decimal Value { get; init; }

        public double Width { get; init; }

        public string Caption { get; init; } = string.Empty;

        public Brush Fill { get; init; } = Brushes.Transparent;

        public string ValueLabel => Value.ToString("N0");
    }

    public sealed class DashboardTimelineStage
    {
        public string Stage { get; init; } = string.Empty;

        public string Detail { get; init; } = string.Empty;

        public int StartColumn { get; init; }

        public int Span { get; init; } = 1;

        public string DurationLabel { get; init; } = string.Empty;

        public Brush Fill { get; init; } = Brushes.Transparent;
    }
}
