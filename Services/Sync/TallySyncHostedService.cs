using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace Acczite20.Services.Sync
{
    public class TallySyncHostedService : BackgroundService
    {
        private readonly ILogger<TallySyncHostedService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SyncStateMonitor _stateMonitor;
        private readonly IConfiguration _configuration;

        // Poll every 5 minutes. A 15-second interval was too aggressive — if the
        // sync takes longer than 15s (common for historical data), the next trigger
        // fires before the current one finishes, stacking requests into Tally.
        private readonly TimeSpan _syncInterval = TimeSpan.FromMinutes(5);

        private readonly ISyncControlService _control;

        public TallySyncHostedService(ILogger<TallySyncHostedService> logger, IServiceScopeFactory scopeFactory, SyncStateMonitor stateMonitor, ISyncControlService control, IConfiguration configuration)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _stateMonitor = stateMonitor;
            _control = control;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Tally Sync Background Service starting.");
            _stateMonitor.AddLog("Tally Sync Background Service starting.", "INFO");

            // Wait until session is properly configured before first sync.
            // Support both SQL (Guid) and MongoDB (ObjectId/string) contexts.
            while (SessionManager.Instance.OrganizationId == Guid.Empty && 
                   string.IsNullOrEmpty(SessionManager.Instance.OrganizationObjectId))
            {
                await Task.Delay(2000, stoppingToken);
            }

            // Give Tally time to fully initialize before first background sync
            _stateMonitor.AddLog("Waiting 30s before first background sync to let Tally stabilize...", "INFO");
            await Task.Delay(30_000, stoppingToken);

            // Start Watchdog Worker
            _ = Task.Run(() => RunWatchdogAsync(stoppingToken), stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                var orgs = new[] { SessionManager.Instance.OrganizationId }; // Future: Get all active orgs
                foreach (var orgId in orgs)
                {
                    if (orgId == Guid.Empty) continue;

                    var state = _control.GetState(orgId);
                    if (!state.IsContinuous) continue;

                    // Hosted Service generates its own RunId
                    var runId = Guid.NewGuid();
                    
                    try
                    {
                        using var scope = _scopeFactory.CreateScope();
                        var orchestrator = scope.ServiceProvider.GetRequiredService<TallySyncOrchestrator>();
                        
                        // Orchestrator handles TryStart, Heartbeats, and Completion internally
                        await orchestrator.RunSyncCycleInternalAsync(runId, stoppingToken, owner: SyncOwner.HostedService);
                        
                        _stateMonitor.LastBackgroundSync = DateTime.Now;
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Background sync failed for Org {OrgId}", orgId);
                    }
                }

                await Task.Delay(_syncInterval, stoppingToken);
            }

            _logger.LogInformation("Tally Sync Background Service stopping.");
            _stateMonitor.AddLog("Tally Sync Background Service stopping.", "INFO");

            // Edge Case: App shutdown during sync
            // Prevent orphan threads
            var shutdownOrgs = new[] { SessionManager.Instance.OrganizationId };
            foreach (var org in shutdownOrgs)
            {
                if (org != Guid.Empty)
                    _control.CancelSync(org);
            }
        }
        // Stages that are expected to produce written records.
        // Zero-progress enforcement only applies when the sync is in one of these phases.
        // Fetch/master/validation phases are excluded — they can take time without writing.
        private static readonly string[] _progressEligibleStagePrefixes =
        [
            "Saving",          // VoucherSyncDbWriter flush
            "Streaming",       // VoucherSyncController channel loop
            "Processing",      // any mid-pipeline stage
        ];

        private static bool IsProgressEligibleStage(string phase) =>
            !string.IsNullOrEmpty(phase) &&
            _progressEligibleStagePrefixes.Any(p => phase.StartsWith(p, StringComparison.OrdinalIgnoreCase));

        private async Task RunWatchdogAsync(CancellationToken ct)
        {
            _logger.LogInformation("Sync Watchdog thread started.");
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var orgId = SessionManager.Instance.OrganizationId;
                    if (orgId != Guid.Empty)
                    {
                        var state = _control.GetState(orgId);
                        if (state.Status == SyncLifecycle.Running || state.Status == SyncLifecycle.Starting)
                        {
                            var runId = state.CurrentRunId ?? Guid.Empty;
                            var runIdShort = runId.ToString().Substring(0, 8);
                            var logPrefix = $"[Org:{orgId}][Run:{runIdShort}] ";

                            var now = DateTime.UtcNow;
                            var elapsed = now - state.StartedAt;

                            // ── Check 1: EARLY-FAIL ────────────────────────────────────────────
                            // Sync started but never emitted a heartbeat past TryStart.
                            // After 2 minutes this almost always means Tally is unreachable.
                            if (state.LastHeartbeat == null && now > state.FirstHeartbeatDeadline)
                            {
                                _logger.LogWarning(
                                    "[Org:{OrgId}][Run:{RunId}] WATCHDOG EARLY-FAIL → No heartbeat within 2m of start. Tally may be unresponsive.",
                                    orgId, runIdShort);
                                _stateMonitor.AddLog(
                                    $"{logPrefix}WATCHDOG EARLY-FAIL → No heartbeat within 2m. Tally unresponsive or XML frozen. Aborting.",
                                    "WARNING", "WATCHDOG");

                                _control.CancelSync(orgId);
                                await Task.Delay(5000, ct);
                                if (state.CurrentRunId == runId)
                                    _control.Complete(orgId, runId, SyncLifecycle.Failed);
                            }
                            else
                            {
                                // ── Check 2: FULL TIMEOUT ──────────────────────────────────────
                                // No heartbeat at all for longer than the configured window.
                                var timeoutMinutes = _configuration.GetValue<int>("Sync:WatchdogTimeoutMinutes", 60);
                                var inactiveTime = now - (state.LastHeartbeat ?? state.StartedAt);

                                if (inactiveTime.TotalMinutes > timeoutMinutes)
                                {
                                    _logger.LogWarning(
                                        "[Org:{OrgId}][Run:{RunId}] WATCHDOG TIMEOUT → Cancelled after {Min}m inactive (LastHeartbeat: {HB:HH:mm:ss}, Stage: {Stage})",
                                        orgId, runIdShort, (int)inactiveTime.TotalMinutes, state.LastHeartbeat, state.CurrentPhase);
                                    _stateMonitor.AddLog(
                                        $"{logPrefix}WATCHDOG TIMEOUT → Cancelled after {(int)inactiveTime.TotalMinutes}m with no heartbeat. Stage: {state.CurrentPhase}",
                                        "WARNING", "WATCHDOG");

                                    _control.CancelSync(orgId);
                                    await Task.Delay(5000, ct);
                                    if (state.CurrentRunId == runId)
                                    {
                                        _logger.LogCritical(
                                            "WATCHDOG: Sync [RunId={RunId}] timed out. Forcing lease release.", runId);
                                        _control.Complete(orgId, runId, SyncLifecycle.Failed);
                                    }
                                }

                                // ── Check 3: ZERO-PROGRESS ────────────────────────────────────
                                // Heartbeat is alive but no records written. Only enforced when:
                                //   a) the sync has been running at least 2 minutes (avoids false
                                //      positives during fetch/parse warm-up phases)
                                //   b) the current stage is one that should be producing records
                                //      (Saving / Streaming / Processing). Fetch, master-sync, and
                                //      validation phases are deliberately excluded.
                                else if (state.Status == SyncLifecycle.Running
                                         && elapsed.TotalMinutes >= 2
                                         && IsProgressEligibleStage(state.CurrentPhase))
                                {
                                    var zeroProgressMinutes = _configuration.GetValue<int>("Sync:ZeroProgressTimeoutMinutes", 10);
                                    var stallTime = now - state.LastProgressTime;

                                    if (stallTime.TotalMinutes > zeroProgressMinutes)
                                    {
                                        var lastCompleted = string.IsNullOrEmpty(state.LastCompletedStage)
                                            ? "none"
                                            : state.LastCompletedStage;

                                        _logger.LogWarning(
                                            "[Org:{OrgId}][Run:{RunId}] WATCHDOG ZERO-PROGRESS → Heartbeat alive but 0 new records for {Min}m | Stage: {Stage} | LastCompleted: {LastCompleted} | Count: {Count} → {Count} (+0)",
                                            orgId, runIdShort, (int)stallTime.TotalMinutes, state.CurrentPhase, lastCompleted, state.LastProgressCount, state.LastProgressCount);
                                        _stateMonitor.AddLog(
                                            $"{logPrefix}WATCHDOG ZERO-PROGRESS → No records for {(int)stallTime.TotalMinutes}m | Stage: {state.CurrentPhase} | LastCompleted: {lastCompleted} | Count: {state.LastProgressCount} → {state.LastProgressCount} (+0). Possible: stuck parse loop or DB deadlock.",
                                            "WARNING", "WATCHDOG");

                                        _control.CancelSync(orgId);
                                        await Task.Delay(5000, ct);
                                        if (state.CurrentRunId == runId)
                                            _control.Complete(orgId, runId, SyncLifecycle.Failed);
                                    }
                                }
                            } // end else (has heartbeat)
                        } // end if (Running || Starting)
                    } // end if (orgId != Guid.Empty)
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Sync Watchdog error.");
                }

                await Task.Delay(TimeSpan.FromMinutes(1), ct);
            }
        }
    }
}
