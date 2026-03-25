using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Acczite20.Services.Sync
{
    public class TallySyncHostedService : BackgroundService
    {
        private readonly ILogger<TallySyncHostedService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SyncStateMonitor _stateMonitor;

        // Poll every 5 minutes. A 15-second interval was too aggressive — if the
        // sync takes longer than 15s (common for historical data), the next trigger
        // fires before the current one finishes, stacking requests into Tally.
        private readonly TimeSpan _syncInterval = TimeSpan.FromMinutes(5);

        // Prevents re-entrant sync cycles if a previous run is still in progress.
        private volatile bool _syncInProgress;

        public TallySyncHostedService(ILogger<TallySyncHostedService> logger, IServiceScopeFactory scopeFactory, SyncStateMonitor stateMonitor)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _stateMonitor = stateMonitor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Tally Sync Background Service starting.");
            _stateMonitor.AddLog("Tally Sync Background Service starting.", "INFO");

            // Wait until session is properly configured before first sync.
            while (SessionManager.Instance.OrganizationId == Guid.Empty)
            {
                await Task.Delay(2000, stoppingToken);
            }

            // Give Tally time to fully initialize before first background sync
            _stateMonitor.AddLog("Waiting 30s before first background sync to let Tally stabilize...", "INFO");
            await Task.Delay(30_000, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                if (_syncInProgress)
                {
                    _stateMonitor.AddLog("⏭ Skipping sync cycle — previous cycle still in progress.", "DEBUG");
                }
                else
                {
                    _syncInProgress = true;
                    try
                    {
                        var msg = "Starting background sync cycle...";
                        _stateMonitor.AddLog(msg, "DEBUG");
                        System.Windows.Application.Current?.Dispatcher.Invoke(() => ((App)System.Windows.Application.Current).LogBreadcrumb($"[BackgroundSync] {msg}"));

                        using var scope = _scopeFactory.CreateScope();
                        var orchestrator = scope.ServiceProvider.GetRequiredService<TallySyncOrchestrator>();
                        await orchestrator.RunSyncCycleAsync(stoppingToken);

                        _stateMonitor.AddLog("Sync cycle successful.", "SUCCESS");
                        System.Windows.Application.Current?.Dispatcher.Invoke(() => ((App)System.Windows.Application.Current).LogBreadcrumb("[BackgroundSync] Sync cycle successful."));
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Sync failure");
                        _stateMonitor.AddLog($"Sync failure: {ex.Message}", "ERROR");
                    }
                    finally
                    {
                        _syncInProgress = false;
                    }
                }

                await Task.Delay(_syncInterval, stoppingToken);
            }

            _logger.LogInformation("Tally Sync Background Service stopping.");
            _stateMonitor.AddLog("Tally Sync Background Service stopping.", "INFO");
        }
    }
}
