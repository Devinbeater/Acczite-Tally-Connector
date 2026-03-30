using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using System.Windows.Media;
using Acczite20.Services.Authentication;
using Acczite20.Services.Sync;
using Acczite20.Views.Pages;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using MongoDB.Bson;

namespace Acczite20.Views
{
    public partial class MainWindow : Window
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly SyncStateMonitor _syncMonitor;

        public MainWindow(IServiceProvider serviceProvider)
        {
            (Application.Current as App)?.LogBreadcrumb("MainWindow constructor started");
            _serviceProvider = serviceProvider;
            _syncMonitor = serviceProvider.GetRequiredService<SyncStateMonitor>();
            InitializeComponent();
            try { this.Icon = new System.Windows.Media.Imaging.BitmapImage(new Uri("pack://application:,,,/Assets/app.ico")); } catch { }
            Loaded += MainWindow_Loaded;
            Closed += MainWindow_Closed;
            MainFrame.Navigated += MainFrame_Navigated;
            (Application.Current as App)?.LogBreadcrumb("MainWindow constructor finished");
        }

        private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
        {
            _syncMonitor.PropertyChanged += SyncMonitor_PropertyChanged;
            UpdateSyncBadge();
            (Application.Current as App)?.LogBreadcrumb("MainWindow_Loaded started");
            await RestoreSessionContext();
            (Application.Current as App)?.LogBreadcrumb("MainWindow_Loaded finished");
        }

        private void MainWindow_Closed(object? sender, EventArgs e)
        {
            _syncMonitor.PropertyChanged -= SyncMonitor_PropertyChanged;
            MainFrame.Navigated -= MainFrame_Navigated;
        }

        public async Task RestoreSessionContext()
        {
            var session = Acczite20.Services.SessionManager.Instance;
            if (!session.IsAuthenticated)
            {
                (Application.Current as App)?.LogBreadcrumb("No active session or not authenticated, navigating to LoginPage");
                SetShellVisibility(false);
                NavigateTo<LoginPage>("Login");
                return;
            }

            (Application.Current as App)?.LogBreadcrumb($"Applying permissions for role: {session.Authority}");
            ApplyPermissions(session.Authority);
            
            (Application.Current as App)?.LogBreadcrumb("Loading organizations");
            await LoadOrganizationsAsync();

            if (session.IsTrialExpired)
            {
                (Application.Current as App)?.LogBreadcrumb("Trial expired, navigating to SubscriptionPage");
                NavigateTo<SubscriptionPage>("Subscription");
                MessageBox.Show("Your trial has expired. Please upgrade to continue using all features.", "Trial Expired", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            // Default landing page after login is the dashboard.
            if (MainFrame.Content == null || MainFrame.Content is LoginPage)
            {
                (Application.Current as App)?.LogBreadcrumb("Navigating to DashboardPage");
                NavigateTo<DashboardPage>("Dashboard");
            }

            SetShellVisibility(true);
        }

        private void MainFrame_Navigated(object? sender, NavigationEventArgs e)
        {
            SetShellVisibility(e.Content is not LoginPage and not RegisterPage);
        }

        private void SetShellVisibility(bool isVisible)
        {
            SidebarPanel.Visibility = isVisible ? Visibility.Visible : Visibility.Collapsed;
            HeaderBar.Visibility = isVisible ? Visibility.Visible : Visibility.Collapsed;
            SidebarColumn.Width = isVisible ? new GridLength(240) : new GridLength(0);
            HeaderRow.Height = isVisible ? new GridLength(64) : new GridLength(0);
        }

        private void SyncMonitor_PropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (e.PropertyName is nameof(SyncStateMonitor.IsSyncing)
                or nameof(SyncStateMonitor.CurrentStage)
                or nameof(SyncStateMonitor.TotalRecordsSynced)
                or nameof(SyncStateMonitor.ProgressPercent)
                or nameof(SyncStateMonitor.IsProgressIndeterminate))
            {
                Dispatcher.Invoke(UpdateSyncBadge);
            }
        }

        private void UpdateSyncBadge()
        {
            if (_syncMonitor.IsSyncing)
            {
                SyncStatusBadge.Background = BrushFromHex("#DBEAFE");
                SyncIndicatorDot.Fill = BrushFromHex("#2563EB");
                SyncStatusText.Foreground = BrushFromHex("#1D4ED8");
                SyncStatusText.Text = $"{_syncMonitor.CurrentStage} - {_syncMonitor.TotalRecordsSynced:N0}";
                SyncHeaderProgress.Visibility = Visibility.Visible;
                SyncHeaderProgress.IsIndeterminate = _syncMonitor.IsProgressIndeterminate;
                SyncHeaderProgress.Value = _syncMonitor.IsProgressIndeterminate ? 0 : _syncMonitor.ProgressPercent;
                return;
            }

            SyncHeaderProgress.Visibility = Visibility.Collapsed;
            SyncHeaderProgress.IsIndeterminate = false;
            SyncHeaderProgress.Value = 0;

            switch (_syncMonitor.CurrentStage)
            {
                case "Sync complete":
                    SyncStatusBadge.Background = BrushFromHex("#DCFCE7");
                    SyncIndicatorDot.Fill = BrushFromHex("#16A34A");
                    SyncStatusText.Foreground = BrushFromHex("#166534");
                    SyncStatusText.Text = $"{_syncMonitor.TotalRecordsSynced:N0} records synced";
                    break;
                case "Sync failed":
                    SyncStatusBadge.Background = BrushFromHex("#FEE2E2");
                    SyncIndicatorDot.Fill = BrushFromHex("#DC2626");
                    SyncStatusText.Foreground = BrushFromHex("#991B1B");
                    SyncStatusText.Text = "Sync failed";
                    break;
                case "Sync cancelled":
                    SyncStatusBadge.Background = BrushFromHex("#FEF3C7");
                    SyncIndicatorDot.Fill = BrushFromHex("#D97706");
                    SyncStatusText.Foreground = BrushFromHex("#92400E");
                    SyncStatusText.Text = "Sync cancelled";
                    break;
                default:
                    SyncStatusBadge.Background = BrushFromHex("#D1FAE5");
                    SyncIndicatorDot.Fill = BrushFromHex("#059669");
                    SyncStatusText.Foreground = BrushFromHex("#065F46");
                    SyncStatusText.Text = "Synced";
                    break;
            }
        }

        // Pre-built frozen brushes for the sync badge — avoids allocating new objects on every
        // property-change event fired by SyncStateMonitor during an active sync.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SolidColorBrush> _brushCache = new();
        private static SolidColorBrush BrushFromHex(string hex) =>
            _brushCache.GetOrAdd(hex, static h =>
            {
                var brush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(h));
                brush.Freeze(); // Makes the brush immutable and thread-safe
                return brush;
            });

        private async Task LoadOrganizationsAsync()
        {
            try
            {
                var session = Acczite20.Services.SessionManager.Instance;
                var orgs = new System.Collections.Generic.List<object>();
                
                // Add only the current session organization as requested
                if (!string.IsNullOrEmpty(session.OrganizationObjectId) || session.OrganizationId != Guid.Empty)
                {
                    var currentId = !string.IsNullOrEmpty(session.OrganizationObjectId) ? session.OrganizationObjectId : session.OrganizationId.ToString();
                    orgs.Add(new { OrganizationId = currentId, CompanyName = session.OrganizationName ?? "Current Organization" });
                }

                OrgSwitcher.ItemsSource = orgs;
                OrgSwitcher.DisplayMemberPath = "CompanyName";
                OrgSwitcher.SelectedValuePath = "OrganizationId";

                if (orgs.Count > 0)
                {
                    OrgSwitcher.SelectedIndex = 0;
                }
            }
            catch (Exception ex)
            {
                (Application.Current as App)?.LogBreadcrumb($"Error loading organizations: {ex.Message}");
            }
        }

        private async void OrgSwitcher_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            var selectedValue = OrgSwitcher.SelectedValue?.ToString();
            if (string.IsNullOrEmpty(selectedValue)) return;

            var session = Acczite20.Services.SessionManager.Instance;
            bool changed = false;

            if (Guid.TryParse(selectedValue, out var newGuidId))
            {
                if (session.OrganizationId != newGuidId)
                {
                    session.OrganizationId = newGuidId;
                    session.OrganizationObjectId = string.Empty; // Clear MongoDB ID if we switched to SQL org
                    changed = true;
                }
            }
            else
            {
                // Likely a MongoDB ObjectId or other string ID
                if (session.OrganizationObjectId != selectedValue)
                {
                    session.OrganizationObjectId = selectedValue;
                    // Generate deterministic GUID for local DB identity
                    using var sha = System.Security.Cryptography.SHA256.Create();
                    var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(selectedValue));
                    var guidBytes = new byte[16];
                    Array.Copy(hash, guidBytes, 16);
                    guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x50);
                    guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);
                    session.OrganizationId = new Guid(guidBytes);
                    changed = true;
                }
            }

            if (changed)
            {
                UpdateSessionOrganizationNameFromSelection(session);
                await PersistSessionAsync(session);

                // Reload current page to reflect new org context
                if (MainFrame.Content is Page currentPage)
                {
                    var pageType = currentPage.GetType();
                    // Don't reload if it's the LoginPage
                    if (pageType != typeof(LoginPage))
                    {
                        var method = typeof(MainWindow).GetMethod(nameof(NavigateTo), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                        var genericMethod = method?.MakeGenericMethod(pageType);
                        genericMethod?.Invoke(this, new object[] { ActivePageTitle.Text });
                    }
                }
            }
        }

        private void UpdateSessionOrganizationNameFromSelection(Acczite20.Services.SessionManager session)
        {
            var selectedItem = OrgSwitcher.SelectedItem;
            if (selectedItem == null)
            {
                return;
            }

            var nameProp = selectedItem.GetType().GetProperty("CompanyName");
            var organizationName = nameProp?.GetValue(selectedItem)?.ToString();
            if (!string.IsNullOrWhiteSpace(organizationName))
            {
                session.OrganizationName = organizationName;
            }
        }

        private async System.Threading.Tasks.Task PersistSessionAsync(Acczite20.Services.SessionManager session)
        {
            if (!session.IsAuthenticated || string.IsNullOrWhiteSpace(session.JwtToken))
            {
                return;
            }

            try
            {
                var persistenceService = _serviceProvider.GetService<SessionPersistenceService>();
                if (persistenceService == null)
                {
                    return;
                }

                await persistenceService.SaveSessionAsync(new UserSessionData
                {
                    Token = session.JwtToken ?? string.Empty,
                    UserId = session.UserId != Guid.Empty ? session.UserId.ToString() : session.UserObjectId,
                    UserObjectId = session.UserObjectId,
                    Username = session.Username ?? string.Empty,
                    OrganizationId = session.OrganizationId != Guid.Empty ? session.OrganizationId.ToString() : session.OrganizationObjectId,
                    OrganizationObjectId = session.OrganizationObjectId,
                    OrganizationName = session.OrganizationName,
                    Authority = session.Authority ?? string.Empty,
                    IsTrialExpired = session.IsTrialExpired
                });
            }
            catch (Exception ex)
            {
                (Application.Current as App)?.LogBreadcrumb($"Session persistence error: {ex.Message}");
            }
        }

        private void ApplyPermissions(string? authority)
        {
            // Normalize authority
            string role = (authority ?? "viewer").ToLower();

            // Default: Everyone sees Pulse and Sync Status
            NavPulseButton.Visibility = Visibility.Visible;
            NavDashboardButton.Visibility = Visibility.Visible;

            // Admin / SuperAdmin: See everything
            bool isAdmin = role == "admin" || role == "superadmin";
            
            // Accountant: Financials only
            bool isAccountant = role == "accountant";

            // Viewer: Dashboard only
            bool isViewer = role == "viewer";

            // Financial Explorers (Accountant & Admin)
            Visibility financialVisibility = (isAdmin || isAccountant) ? Visibility.Visible : Visibility.Collapsed;
            NavVoucherButton.Visibility = financialVisibility;
            NavDaybookButton.Visibility = financialVisibility;
            NavLedgerButton.Visibility = financialVisibility;
            NavTrialBalanceButton.Visibility = financialVisibility;
            NavPandLButton.Visibility = financialVisibility;
            NavBalanceSheetButton.Visibility = financialVisibility;
            NavInventoryButton.Visibility = financialVisibility;
            NavGstButton.Visibility = financialVisibility;

            // Management Tools (Admin only)
            Visibility adminVisibility = isAdmin ? Visibility.Visible : Visibility.Collapsed;
            NavSyncButton.Visibility = adminVisibility;
            NavSyncMonitorButton.Visibility = adminVisibility;
            NavRiskButton.Visibility = adminVisibility;
            NavAttendanceButton.Visibility = adminVisibility;
            NavPayrollButton.Visibility = adminVisibility;
            NavSettingsButton.Visibility = adminVisibility;
            NavLogsButton.Visibility = adminVisibility;
            NavReportsButton.Visibility = adminVisibility;
            NavHrButton.Visibility = adminVisibility;
            NavAdminInventoryButton.Visibility = adminVisibility;
        }

        private void NavPulse_Click(object sender, RoutedEventArgs e) => NavigateTo<ExecutiveDashboardPage>("Business Pulse");
        private void NavDashboard_Click(object sender, RoutedEventArgs e) => NavigateTo<DashboardPage>("Dashboard");
        private void NavTimeline_Click(object sender, RoutedEventArgs e) => NavigateTo<TimelinePage>("Unified Global Timeline");
        private void NavVoucherExplorer_Click(object sender, RoutedEventArgs e) => NavigateTo<VoucherExplorerPage>("Voucher Explorer");
        private void NavDaybook_Click(object sender, RoutedEventArgs e) => NavigateTo<DaybookPage>("Daybook");
        private void NavLedgerExplorer_Click(object sender, RoutedEventArgs e) => NavigateTo<LedgerExplorerPage>("Ledger Explorer");
        private void NavTrialBalance_Click(object sender, RoutedEventArgs e) => NavigateTo<TrialBalancePage>("Trial Balance");
        private void NavPandL_Click(object sender, RoutedEventArgs e) => NavigateTo<ProfitAndLossPage>("Profit & Loss Account");
        private void NavBalanceSheet_Click(object sender, RoutedEventArgs e) => NavigateTo<BalanceSheetPage>("Balance Sheet");
        private void NavInventory_Click(object sender, RoutedEventArgs e) => NavigateTo<InventoryExplorerPage>("Inventory Explorer");
        private void NavGst_Click(object sender, RoutedEventArgs e) => NavigateTo<GstReportingPage>("GST Health Report");
        private void NavAttendance_Click(object sender, RoutedEventArgs e) => NavigateToHrSection("Employee Attendance", 0);
        private void NavPayroll_Click(object sender, RoutedEventArgs e) => NavigateToHrSection("Payroll", 1);
        private void NavHr_Click(object sender, RoutedEventArgs e) => NavigateToHrSection("HR Management", 0);
        private void NavAdminInventory_Click(object sender, RoutedEventArgs e) => NavigateTo<AdminInventoryPage>("Admin Inventory");
        private void NavSync_Click(object sender, RoutedEventArgs e)
        {
            if (!System.IO.File.Exists("dbconfig.json"))
            {
                NavigateTo<DatabaseConnectionPage>("Database Configuration");
                return;
            }

            if (!System.IO.File.Exists("tallyconfig.json") && !Acczite20.Services.SessionManager.Instance.IsTallyConnected)
            {
                var result = MessageBox.Show(
                    "Tally connection has not been configured yet.\n\nWould you like to go to Settings to connect Tally first?",
                    "Tally Not Connected",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Question);

                if (result == MessageBoxResult.Yes)
                {
                    NavigateTo<SettingsPage>("Settings");
                    return;
                }
            }

            NavigateTo<TallySyncPage>("Tally Sync Control");
        }
        private void NavSyncMonitor_Click(object sender, RoutedEventArgs e) => NavigateTo<UnifiedSyncPage>("Enterprise Integration Hub");
        private void NavRisk_Click(object sender, RoutedEventArgs e) => NavigateTo<RiskMonitorPage>("Customer Risk Monitor");
        private void NavReports_Click(object sender, RoutedEventArgs e) => NavigateTo<ReportsPage>("Reports");
        private void NavLogs_Click(object sender, RoutedEventArgs e) => NavigateTo<LogsPage>("System Logs");
        private void NavSettings_Click(object sender, RoutedEventArgs e) => NavigateTo<SettingsPage>("Settings");
        private void NavSubscription_Click(object sender, RoutedEventArgs e) => NavigateTo<SubscriptionPage>("Subscription");
        private void NavSearch_Click(object sender, RoutedEventArgs e) => NavigateTo<GlobalSearchPage>("Global Enterprise Search");
        
        private async void Logout_Click(object sender, RoutedEventArgs e)
        {
            var persistenceService = _serviceProvider.GetService<Acczite20.Services.Authentication.SessionPersistenceService>();
            persistenceService?.ClearSession();

            Acczite20.Services.SessionManager.Instance.ClearSession();
            Hide();

            var startupWindow = new StartupWindow();
            var startupResult = startupWindow.ShowDialog();
            if (startupResult == true)
            {
                Show();
                await RestoreSessionContext();
                return;
            }

            Close();
        }

        private void NavigateTo<T>(string title) where T : System.Windows.Controls.Page
        {
            try
            {
                if (MainFrame != null)
                {
                    var page = _serviceProvider.GetRequiredService<T>();
                    MainFrame.Navigate(page);
                    ActivePageTitle.Text = title;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Navigation failed: {ex.Message}", "Error");
            }
        }

        private void NavigateToHrSection(string title, int tabIndex)
        {
            try
            {
                if (MainFrame != null)
                {
                    var page = _serviceProvider.GetRequiredService<HrManagementPage>();
                    page.SetInitialTab(tabIndex);
                    MainFrame.Navigate(page);
                    ActivePageTitle.Text = title;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Navigation failed: {ex.Message}", "Error");
            }
        }

        public void SetActivePageTitle(string title)
        {
            ActivePageTitle.Text = title;
        }
    }
}

