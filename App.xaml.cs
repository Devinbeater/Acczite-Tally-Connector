using System;
using System.Windows;
using Microsoft.Extensions.DependencyInjection;
using Acczite20.Views;
using Acczite20.Views.Pages;
using Acczite20.Services.Navigation;
using Acczite20.Data;
using Acczite20.Services;
using Acczite20.Services.Sync;
using Acczite20.Services.Integration;
using Acczite20.Services.History;
using Acczite20.Services.Analytics;
using MongoDB.Driver;
using MongoDB.Bson;
using Microsoft.EntityFrameworkCore;
using Acczite20.Services.Dashboard;
using Acczite20.Services.Explorer;
using Acczite20.Services.Reports;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows.Threading;
using Microsoft.Extensions.Configuration;
using System.Net.Http;
using Acczite20.Services.Authentication;
using Acczite20.Services.Tally;
using System.Runtime.InteropServices;

namespace Acczite20
{
    public partial class App : Application
    {
        public static new App Current => (App)Application.Current;
        public IServiceProvider ServiceProvider { get; private set; }
        public static INavigationService? NavigationService { get; private set; }

        public App()
        {
            LogBreadcrumb("App constructor started");
            InitializeComponent();
            var services = new ServiceCollection();
            ConfigureServices(services);
            ServiceProvider = services.BuildServiceProvider();

            // Wire EventBus to subscribers
            LogBreadcrumb("Registering event subscribers");
            EventQueueSubscriber.RegisterAll(ServiceProvider);
            TimelineEventSubscriber.RegisterAll(ServiceProvider);

            // Route EventBus handler errors to the crash log instead of silently swallowing them
            Core.Events.EventBus.OnHandlerError = (eventTypeName, correlationId, ex) =>
                LogBreadcrumb($"[EventBus] Handler error — event={eventTypeName} correlationId={correlationId} error={ex.Message}");

            DispatcherUnhandledException += OnDispatcherUnhandledException;
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
            LogBreadcrumb("App constructor finished");
        }

        private void ConfigureServices(IServiceCollection services)
        {
            LoadSessionConfig();

            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();
            services.AddSingleton<IConfiguration>(configuration);

            services.AddSingleton<HttpClient>();
            services.AddSingleton<IAuthenticationService, AuthenticationService>();
            services.AddSingleton<SessionPersistenceService>();
            services.AddSingleton<OrganizationContextService>();
            
            services.AddSingleton<SessionManager>(sp => SessionManager.Instance);

            services.AddSingleton<IMongoClient>(sp =>
            {
                // Default to localhost — MongoClient is lazy, won't error if Mongo isn't running.
                // Empty string would throw MongoConfigurationException on construction (breaks MySQL-only users).
                string mongoUri = "mongodb://root:wcOxy2nU4OZXA6Ze5RFp03NSL4bifE7VBRe2AkYLC2zq50olDxW8tqxErINBY9go@72.62.74.72:27017/test?authSource=admin";
                if (System.IO.File.Exists("dbconfig.json"))
                {
                    try
                    {
                        var json = System.IO.File.ReadAllText("dbconfig.json");
                        var config = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                        if (config != null && config.TryGetValue("MongoUri", out string? uri) && !string.IsNullOrWhiteSpace(uri))
                        {
                            mongoUri = uri;
                        }
                    }
                    catch (Exception ex) { LogBreadcrumb($"App: Failed to read dbconfig.json for MongoUri: {ex.Message}"); }
                }

                var client = new MongoClient(mongoUri);
                try 
                {
                    var dbs = client.ListDatabaseNames().ToList();
                    LogBreadcrumb($"App: Connected to REMOTE Mongo (72.62...). Available DBs: {string.Join(", ", dbs)}");

                    try 
                    {
                        var localClient = new MongoClient("mongodb://localhost:27017");
                        var localDbs = localClient.ListDatabaseNames().ToList();
                        LogBreadcrumb($"App: Connected to LOCAL Mongo. Available DBs: {string.Join(", ", localDbs)}");
                        
                        if (localDbs.Contains("acczite"))
                        {
                            var adb = localClient.GetDatabase("acczite");
                            var itemsCount = adb.GetCollection<BsonDocument>("stockitems").CountDocuments(new BsonDocument());
                            var orgsCount = adb.GetCollection<BsonDocument>("organizations").CountDocuments(new BsonDocument());
                            LogBreadcrumb($"App: Local 'acczite' has {itemsCount} stockitems and {orgsCount} organizations");
                        }
                    }
                    catch { LogBreadcrumb("App: No LOCAL Mongo found"); }
                }
                catch (Exception ex) { LogBreadcrumb($"App: Mongo DB List Error: {ex.Message}"); }
                return client;
            });

            services.AddSingleton<MongoService>();
            services.AddSingleton<IMongoProjector, MongoProjector>();
            services.AddScoped<ISyncMetadataService, SyncMetadataService>();
            services.AddScoped<IMasterRepository, MasterRepository>();
            services.AddScoped<DeadLetterReplayService>();

            // View Models / Pages
            services.AddSingleton<MainWindow>();
            services.AddSingleton<INavigationService>(provider =>
            {
                var mainWindow = provider.GetRequiredService<MainWindow>();
                return new NavigationService(mainWindow.MainFrame, provider);
            });

            services.AddTransient<LoginPage>();
            services.AddTransient<RegisterPage>();
            services.AddTransient<MainPage>();
            services.AddTransient<DashboardPage>();
            services.AddTransient<VoucherExplorerPage>();
            services.AddTransient<LedgerExplorerPage>();
            services.AddTransient<TrialBalancePage>();
            services.AddTransient<ReportsPage>();
            services.AddTransient<SettingsPage>();
            services.AddTransient<LogsPage>();
            services.AddTransient<DatabaseConnectionPage>();
            services.AddTransient<TallySyncPage>(sp => new TallySyncPage(new System.Collections.Generic.List<string>(), sp.GetRequiredService<INavigationService>()));
            services.AddTransient<TallyExecutePage>(sp => new TallyExecutePage(new System.Collections.Generic.List<string>(), new System.Collections.Generic.List<string>(), sp.GetRequiredService<INavigationService>()));
            services.AddTransient<TableSelectionPage>(sp => new TableSelectionPage(string.Empty, sp.GetRequiredService<INavigationService>()));
            services.AddTransient<SubscriptionPage>();
            services.AddTransient<SyncMonitorPage>();
            services.AddTransient<InventoryExplorerPage>();
            services.AddTransient<GstReportingPage>();
            services.AddTransient<ProfitAndLossPage>();
            services.AddTransient<BalanceSheetPage>();
            services.AddTransient<HrManagementPage>();
            services.AddTransient<AdminInventoryPage>();
            services.AddTransient<ExecutiveDashboardPage>();
            services.AddTransient<UnifiedSyncPage>();
            services.AddTransient<GlobalSearchPage>();
            services.AddTransient<MappingReviewPage>();
            services.AddTransient<TimelinePage>();
            services.AddTransient<RiskMonitorPage>();
            services.AddTransient<ComparativePandLPage>();
            services.AddTransient<AnomalyDetectionPage>();
            services.AddTransient<DaybookPage>();

            // Business Logic Services
            services.AddTransient<DashboardService>();
            services.AddTransient<DaybookService>();
            services.AddTransient<VoucherExplorerService>();
            services.AddTransient<LedgerExplorerService>();
            services.AddTransient<TrialBalanceService>();
            services.AddTransient<GstReportService>();
            services.AddTransient<InventoryExplorerService>();
            services.AddTransient<HrService>();
            services.AddTransient<AdminService>();
            services.AddTransient<PandLService>();
            services.AddTransient<BalanceSheetService>();
            services.AddTransient<FinancialHealthService>();
            services.AddTransient<LedgerDrillDownService>();
            
            // Advanced Analytics & Integrity
            services.AddScoped<ITimelineService, TimelineService>();
            services.AddScoped<ICustomerRiskService, CustomerRiskService>();
            services.AddScoped<IFinancialAnalysisService, FinancialAnalysisService>();
            services.AddSingleton<IGlobalSearchService, GlobalSearchService>();
            services.AddScoped<IBusinessPulseService, BusinessPulseService>();
            services.AddScoped<IReportingService, ReportingService>();
            services.AddScoped<IEntityMappingService, EntityMappingService>();
            services.AddScoped<IMernIntegrationService, MernIntegrationService>();

            // Sync Module Services
            services.AddSingleton<TallyXmlService>();
            services.AddSingleton<TallyCompanyService>();
            services.AddSingleton<TallyXmlParser>();
            services.AddSingleton<SyncStateMonitor>();
            services.AddSingleton<TallyOdbcImporter>();
            services.AddScoped<TallyMasterSyncService>();
            services.AddSingleton<ISyncLockProvider, LocalSyncLockProvider>();
            services.AddSingleton<ISyncControlService, SyncControlService>();
            services.AddScoped<TallySyncOrchestrator>();
            services.AddScoped<Acczite20.Infrastructure.MasterDataCache>();
            services.AddScoped<Acczite20.Infrastructure.BulkInsertHandler>();
            services.AddScoped<Acczite20.Services.Sync.LedgerSnapshotService>();
            
            // Background Event Dispatcher
            services.AddSingleton<IntegrationEventDispatcher>();

            services.AddDbContext<AppDbContext>(options =>
            {
                var dbType = SessionManager.Instance.SelectedDatabaseType;
                var connStr = SessionManager.Instance.ConnectionString;
                
                if (!string.IsNullOrWhiteSpace(dbType) && !string.IsNullOrWhiteSpace(connStr)) 
                {
                    Acczite20.Infrastructure.DatabaseProviderFactory.Configure(options, dbType, connStr);
                }
                else
                {
                    // Fallback to avoid crash during resolution
                    options.UseInMemoryDatabase("AccziteFallback");
                }
            });

            // Background Services
            services.AddSingleton<TallySyncHostedService>();
            services.AddLogging();
        }

        private void LoadSessionConfig()
        {
            if (!System.IO.File.Exists("dbconfig.json"))
            {
                return;
            }

            try
            {
                var json = System.IO.File.ReadAllText("dbconfig.json");
                var config = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                if (config == null)
                {
                    return;
                }

                var session = SessionManager.Instance;

                if (config.TryGetValue("DbType", out var dbType) && !string.IsNullOrWhiteSpace(dbType))
                {
                    session.SelectedDatabaseType = dbType;
                }

                if (config.TryGetValue("Database", out var databaseName) && !string.IsNullOrWhiteSpace(databaseName))
                {
                    session.DatabaseName = databaseName;
                }

                if (string.Equals(session.SelectedDatabaseType, "MongoDB", StringComparison.OrdinalIgnoreCase) &&
                    config.TryGetValue("MongoUri", out var mongoUri) &&
                    !string.IsNullOrWhiteSpace(mongoUri))
                {
                    session.ConnectionString = mongoUri;
                    var mongoUrl = MongoUrl.Create(mongoUri);
                    session.DatabaseName ??= string.IsNullOrWhiteSpace(mongoUrl.DatabaseName)
                        ? "acczite_master"
                        : mongoUrl.DatabaseName;
                    return;
                }

                if (string.Equals(session.SelectedDatabaseType, "SQL Server", StringComparison.OrdinalIgnoreCase) &&
                    config.TryGetValue("Server", out var sqlServer) &&
                    config.TryGetValue("Database", out var sqlDatabase) &&
                    config.TryGetValue("Username", out var sqlUsername) &&
                    config.TryGetValue("Password", out var sqlPassword))
                {
                    session.ConnectionString = $"Server={sqlServer};Database={sqlDatabase};User Id={sqlUsername};Password={sqlPassword};TrustServerCertificate=True;";
                    return;
                }

                if (string.Equals(session.SelectedDatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase) &&
                    config.TryGetValue("Server", out var mySqlServer) &&
                    config.TryGetValue("Database", out var mySqlDatabase) &&
                    config.TryGetValue("Username", out var mySqlUsername) &&
                    config.TryGetValue("Password", out var mySqlPassword))
                {
                    var port = config.TryGetValue("Port", out var configuredPort) && !string.IsNullOrWhiteSpace(configuredPort)
                        ? configuredPort
                        : "3306";
                    session.ConnectionString = $"Server={mySqlServer};Port={port};Database={mySqlDatabase};Uid={mySqlUsername};Pwd={mySqlPassword};";
                }
            }
            catch (Exception ex)
            {
                LogBreadcrumb($"LoadSessionConfig failed: {ex.Message}");
            }
        }

        protected override void OnStartup(StartupEventArgs e)
        {
            LogBreadcrumb("OnStartup started");
            try { SetCurrentProcessExplicitAppUserModelID("Acczite.Enterprise.SyncHub.20"); } catch { }
            base.OnStartup(e);
            ModernWpf.ThemeManager.Current.ApplicationTheme = ModernWpf.ApplicationTheme.Light;
            ShutdownMode = ShutdownMode.OnExplicitShutdown;

            try
            {
                if (ServiceProvider == null)
                {
                    LogBreadcrumb("Fatal: ServiceProvider is null");
                    MessageBox.Show("❌ Dependency Injection not initialized.", "Fatal Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    Shutdown(-1);
                    return;
                }

                LogBreadcrumb("Showing StartupWindow");
                var startupWindow = new StartupWindow();
                bool? startupResult = startupWindow.ShowDialog();

                LogBreadcrumb($"StartupWindow result: {startupResult}");
                if (startupResult == true)
                {
                    // Restore User Session before showing MainWindow
                    LogBreadcrumb("Restoring session");
                    var persistence = ServiceProvider.GetRequiredService<SessionPersistenceService>();
                    RestoreSession(persistence);

                    LogBreadcrumb("Resolving MainWindow");
                    var mainWindow = ServiceProvider.GetRequiredService<MainWindow>();
                    Application.Current.MainWindow = mainWindow;
                    NavigationService = ServiceProvider.GetRequiredService<INavigationService>();
                    ShutdownMode = ShutdownMode.OnMainWindowClose;
                    
                    LogBreadcrumb("Showing MainWindow");
                    mainWindow.Show();

                    // Start Background Sync
                    var syncService = ServiceProvider.GetRequiredService<TallySyncHostedService>();
                    _ = syncService.StartAsync(CancellationToken.None);

                    // Start Mongo Projector Loops (Consumption + Fallback Drainer)
                    var projector = ServiceProvider.GetRequiredService<IMongoProjector>();
                    _ = projector.ProcessQueueAsync(CancellationToken.None);
                    _ = projector.DrainFallbackQueueAsync(CancellationToken.None);
                }
                else
                {
                    LogBreadcrumb("Shutting down (StartupResult != true)");
                    Shutdown();
                }
            }
            catch (Exception ex)
            {
                LogCrash(ex, "OnStartup Exception");
                MessageBox.Show($"🚨 Application failed to start:\n{ex}", "Startup Exception", MessageBoxButton.OK, MessageBoxImage.Error);
                Shutdown(-1);
            }
        }

        private void RestoreSession(SessionPersistenceService persistence)
        {
            var session = SessionManager.Instance;
            if (session.IsAuthenticated && !string.IsNullOrWhiteSpace(session.JwtToken))
            {
                return;
            }

            // Sync wait is acceptable here during startup splash
            var sessionData = persistence.LoadSessionAsync().GetAwaiter().GetResult();
            if (sessionData != null && !string.IsNullOrEmpty(sessionData.Token))
            {
                session.JwtToken = sessionData.Token;
                session.IsAuthenticated = true;
                session.Username = sessionData.Username;
                session.Authority = sessionData.Authority;
                session.IsTrialExpired = sessionData.IsTrialExpired;

                if (Guid.TryParse(sessionData.UserId, out var uid)) session.UserId = uid;
                session.UserObjectId = sessionData.UserObjectId;

                if (Guid.TryParse(sessionData.OrganizationId, out var oid)) session.OrganizationId = oid;
                session.OrganizationObjectId = sessionData.OrganizationObjectId;
                session.OrganizationName = sessionData.OrganizationName;
            }
        }

        private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
        {
            LogCrash(e.Exception, "DispatcherUnhandledException");
            MessageBox.Show($"Unexpected UI error:\n{e.Exception}", "Unhandled Exception", MessageBoxButton.OK, MessageBoxImage.Error);
            e.Handled = true;
        }

        private void OnUnhandledException(object? sender, UnhandledExceptionEventArgs e)
        {
            if (e.ExceptionObject is Exception ex)
            {
                LogCrash(ex, "UnhandledException");
                MessageBox.Show($"Unexpected fatal error:\n{ex}", "Unhandled Exception", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            LogCrash(e.Exception, "UnobservedTaskException");
            e.SetObserved();
        }

        public void LogBreadcrumb(string msg)
        {
            try
            {
                var logMsg = $"[{DateTime.Now:HH:mm:ss.fff}] TRACE: {msg}\n";
                System.IO.File.AppendAllText("trace.log", logMsg);
            }
            catch { }
        }

        public void LogCrash(Exception ex, string type)
        {
            try
            {
                var logMsg = $"[{DateTime.Now}] {type}: {ex.ToString()}\n\n";
                System.IO.File.AppendAllText("crash.log", logMsg);
                LogBreadcrumb($"CRASH LOGGED: {type}");
            }
            catch { }
        }

        [DllImport("shell32.dll", SetLastError = true)]
        private static extern int SetCurrentProcessExplicitAppUserModelID([MarshalAs(UnmanagedType.LPWStr)] string appId);
    }
}
