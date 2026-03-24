using System.Threading;

namespace Acczite20.Services.Sync
{
    public readonly record struct VoucherSyncProgressSnapshot(int Fetched, int Written);

    public sealed class VoucherSyncProgressAggregator
    {
        private int _fetched;
        private int _written;

        public void RecordFetched(int count) => Interlocked.Add(ref _fetched, count);

        public void RecordWritten(int count) => Interlocked.Add(ref _written, count);

        public VoucherSyncProgressSnapshot Snapshot()
        {
            return new VoucherSyncProgressSnapshot(
                Volatile.Read(ref _fetched),
                Volatile.Read(ref _written));
        }
    }
}
