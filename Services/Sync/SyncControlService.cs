using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public class SyncControlService : ISyncControlService
    {
        private readonly ConcurrentDictionary<Guid, SyncState> _states = new();
        private readonly ConcurrentDictionary<Guid, object> _locks = new();

        public SyncState GetState(Guid orgId)
        {
            return _states.GetOrAdd(orgId, id => new SyncState
            {
                OrgId = id,
                StartedAt = DateTime.UtcNow
            });
        }

        private object GetLock(Guid orgId) => _locks.GetOrAdd(orgId, _ => new object());

        public bool TryStart(Guid orgId, SyncOwner owner, Guid runId)
        {
            lock (GetLock(orgId))
            {
                var state = GetState(orgId);

                // Permissive re-entry for SAME execution chain
                if (state.Status == SyncLifecycle.Starting || state.Status == SyncLifecycle.Running)
                {
                    if (state.CurrentRunId == runId) return true;
                    return false; // REAL conflict
                }

                state.Cts?.Cancel();
                state.Cts = new CancellationTokenSource();

                state.IsPaused = false;
                state.Status = SyncLifecycle.Starting;
                state.Owner = owner;
                state.CurrentRunId = runId;
                state.StartedAt = DateTime.UtcNow;
                state.LastHeartbeat = DateTime.UtcNow;
                state.FirstHeartbeatDeadline = DateTime.UtcNow.AddMinutes(2);
                state.LastProgressCount = 0;
                state.LastProgressTime = DateTime.UtcNow;
                state.CurrentPhase = "Initializing";

                return true;
            }
        }

        public void EnsureOwnership(Guid orgId, Guid runId)
        {
            var state = GetState(orgId);
            if (state.CurrentRunId != runId)
            {
                throw new InvalidOperationException($"CRITICAL: Sync execution chain [RunId={runId}] lost ownership of Organization [OrgId={orgId}]. Active lease belongs to [RunId={state.CurrentRunId ?? Guid.Empty}]. Execution aborted.");
            }
        }

        public void UpdateHeartbeat(Guid orgId, Guid runId, string stage, int recordsProcessed = 0)
        {
            var state = GetState(orgId);
            if (state.CurrentRunId != runId) return;

            state.LastHeartbeat = DateTime.UtcNow;

            // When the stage changes, the previous stage completed successfully — snapshot it.
            if (!string.IsNullOrEmpty(state.CurrentPhase) && state.CurrentPhase != stage)
                state.LastCompletedStage = state.CurrentPhase;

            state.CurrentPhase = stage;

            // Tweak 2: advance progress cursor only when new records are reported
            if (recordsProcessed > state.LastProgressCount)
            {
                state.LastProgressCount = recordsProcessed;
                state.LastProgressTime = DateTime.UtcNow;
            }
        }

        public void Complete(Guid orgId, Guid runId, SyncLifecycle finalStatus)
        {
            lock (GetLock(orgId))
            {
                var state = GetState(orgId);
                if (state.CurrentRunId == runId)
                {
                    state.Status = finalStatus;
                    state.CurrentRunId = null;
                    state.CurrentPhase = finalStatus == SyncLifecycle.Completed ? "Idle" : $"Terminated ({finalStatus})";
                }
            }
        }

        public async Task PauseAsync(Guid orgId)
        {
            var state = GetState(orgId);

            if (!state.IsPaused)
            {
                state.IsPaused = true;
                state.Status = SyncLifecycle.Paused;

                await state.PauseGate.WaitAsync();
            }
        }

        public void Resume(Guid orgId)
        {
            var state = GetState(orgId);

            if (state.IsPaused)
            {
                if (state.PauseGate.CurrentCount == 0)
                    state.PauseGate.Release();

                state.IsPaused = false;
                state.Status = SyncLifecycle.Running;
            }
        }

        public void CancelSync(Guid orgId)
        {
            var state = GetState(orgId);

            state.Status = SyncLifecycle.Stopping;
            state.Cts.Cancel();
        }

        public SyncLifecycle GetStatus(Guid orgId)
        {
            return GetState(orgId).Status;
        }
    }
}
