using System;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public enum SyncLifecycle
    {
        Idle,
        Running,
        Paused,
        Stopping,
        Faulted
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
    }

    public interface ISyncControlService
    {
        SyncState GetState(Guid orgId);
        bool TryStart(Guid orgId, SyncOwner owner);
        Task PauseAsync(Guid orgId);
        void Resume(Guid orgId);
        void CancelSync(Guid orgId);
        SyncLifecycle GetStatus(Guid orgId);
    }
}
