using System;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public enum SyncLifecycle
    {
        Idle,
        Starting,
        Running,
        Pausing,
        Paused,
        Stopping,
        Completed,
        Failed,
        Cancelled
    }

    public enum SyncOwner
    {
        None,
        HostedService,
        Manual
    }

    public class SyncState
    {
        public Guid OrgId { get; init; }

        public CancellationTokenSource Cts { get; set; } = new();

        public SemaphoreSlim PauseGate { get; } = new(1, 1);

        public volatile bool IsPaused;

        public SyncLifecycle Status { get; set; } = SyncLifecycle.Idle;

        public SyncOwner Owner { get; set; } = SyncOwner.None;

        public string CurrentPhase { get; set; } = string.Empty;

        public DateTime StartedAt { get; set; }
        
        public Guid? CurrentRunId { get; set; }
        
        public DateTime? LastHeartbeat { get; set; }

        // Tweak 1: detect sync that never emits a first heartbeat
        public DateTime FirstHeartbeatDeadline { get; set; }

        // Tweak 2: detect sync that is alive (heartbeating) but making zero progress
        public int LastProgressCount { get; set; }
        public DateTime LastProgressTime { get; set; }

        // Last stage that successfully completed — used in watchdog logs to show
        // where the sync was before it stalled, not just where it is now.
        public string LastCompletedStage { get; set; } = string.Empty;

        public bool IsContinuous { get; set; }
    }

    public interface ISyncControlService
    {
        SyncState GetState(Guid orgId);
        bool TryStart(Guid orgId, SyncOwner owner, Guid runId);
        void EnsureOwnership(Guid orgId, Guid runId);
        void UpdateHeartbeat(Guid orgId, Guid runId, string stage, int recordsProcessed = 0);
        void Complete(Guid orgId, Guid runId, SyncLifecycle finalStatus);
        Task PauseAsync(Guid orgId);
        void Resume(Guid orgId);
        void CancelSync(Guid orgId);
        SyncLifecycle GetStatus(Guid orgId);
    }
}
