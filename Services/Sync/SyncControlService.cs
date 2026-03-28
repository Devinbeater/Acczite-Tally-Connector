using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public class SyncControlService : ISyncControlService
    {
        private readonly ConcurrentDictionary<Guid, SyncState> _states = new();

        public SyncState GetState(Guid orgId)
        {
            return _states.GetOrAdd(orgId, id => new SyncState
            {
                OrgId = id,
                StartedAt = DateTime.UtcNow
            });
        }

        public bool TryStart(Guid orgId, SyncOwner owner)
        {
            var state = GetState(orgId);

            if (state.Status == SyncLifecycle.Running)
                return false;

            state.Cts?.Cancel();
            state.Cts = new CancellationTokenSource();

            state.IsPaused = false;
            state.Status = SyncLifecycle.Running;
            state.Owner = owner;
            state.StartedAt = DateTime.UtcNow;

            return true;
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
