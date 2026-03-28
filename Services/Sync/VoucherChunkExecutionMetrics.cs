using System;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherChunkExecutionMetrics
    {
        public int FetchedCount { get; set; }
        public int EnqueuedCount { get; set; }
        public int RejectedCount { get; set; }
        public TimeSpan Elapsed { get; set; }
        public long PayloadBytes { get; set; }
        public TimeSpan WindowUsed { get; set; }
        public int MaxAlterId { get; set; }
    }
}
