using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherSyncChunkScheduler
    {
        private readonly TimeSpan _minWindow;
        private readonly TimeSpan _maxWindow;
        private readonly TimeSpan _slowThreshold;
        private readonly TimeSpan _fastThreshold;
        private int  _maxVouchersPerChunk;  // mutable — reduced dynamically under sustained overload
        private readonly int _maxRecordsPerChunk;
        private readonly long _largePayloadBytes;
        private readonly long _smallPayloadBytes;
        private TimeSpan _window;

        private const int MinVouchersPerChunkFloor = 50; // never reduce the cap below this

        public VoucherSyncChunkScheduler(
            TimeSpan? initialWindow = null,
            TimeSpan? minWindow = null,
            TimeSpan? maxWindow = null,
            TimeSpan? fastThreshold = null,
            TimeSpan? slowThreshold = null,
            int maxRecordsPerChunk = 150,
            long largePayloadBytes = 16 * 1024 * 1024,
            long smallPayloadBytes = 4 * 1024 * 1024)
        {
            _window = initialWindow ?? TimeSpan.FromDays(1);
            _minWindow = minWindow ?? TimeSpan.FromHours(6);
            _maxWindow = maxWindow ?? TimeSpan.FromDays(3);
            _fastThreshold = fastThreshold ?? TimeSpan.FromSeconds(2);
            _slowThreshold = slowThreshold ?? TimeSpan.FromSeconds(10);
            _maxVouchersPerChunk = maxRecordsPerChunk;
            _maxRecordsPerChunk = maxRecordsPerChunk;
            _largePayloadBytes = largePayloadBytes;
            _smallPayloadBytes = smallPayloadBytes;
        }

        public TimeSpan CurrentWindow      => _window;
        public TimeSpan MinWindow          => _minWindow;
        public int      MaxVouchersPerChunk => _maxVouchersPerChunk;

        /// <summary>
        /// Halves the per-chunk voucher cap (floor: 50).
        /// Called by VoucherSyncController when overload retries exceed the threshold,
        /// meaning even smaller time windows are not helping — we need fewer records.
        /// </summary>
        public void ReduceVoucherCap()
        {
            var reduced = Math.Max(MinVouchersPerChunkFloor, _maxVouchersPerChunk / 2);
            _maxVouchersPerChunk = reduced;
        }

        public async IAsyncEnumerable<DateRange> GetChunksAsync(
            DateTimeOffset from,
            DateTimeOffset to,
            [EnumeratorCancellation] CancellationToken ct)
        {
            if (from > to)
            {
                yield break;
            }

            var current = from;

            while (current <= to)
            {
                ct.ThrowIfCancellationRequested();

                var end = current.Add(_window).AddTicks(-1);
                if (end > to)
                {
                    end = to;
                }

                yield return new DateRange(current, end);

                if (end >= to)
                {
                    yield break;
                }

                current = end.AddTicks(1);
                await Task.Yield();
            }
        }

        public void Adjust(TimeSpan responseTime, int recordCount, long payloadBytes, bool failed = false, bool isSafeMode = false)
        {
            var next = _window;

            if (failed || responseTime >= _slowThreshold || recordCount >= _maxRecordsPerChunk || payloadBytes >= _largePayloadBytes)
            {
                next = Scale(_window, 0.5);
            }
            else if (!isSafeMode && responseTime <= _fastThreshold && recordCount <= _maxRecordsPerChunk / 4 && payloadBytes <= _smallPayloadBytes)
            {
                next = Scale(_window, 2.0);
            }
            else if (!isSafeMode && responseTime <= TimeSpan.FromSeconds(5) && recordCount <= _maxRecordsPerChunk / 2 && payloadBytes <= _largePayloadBytes / 2)
            {
                next = Scale(_window, 1.5);
            }

            _window = Clamp(next);
        }

        private TimeSpan Clamp(TimeSpan value)
        {
            if (value < _minWindow)
            {
                return _minWindow;
            }

            if (value > _maxWindow)
            {
                return _maxWindow;
            }

            return value;
        }

        private static TimeSpan Scale(TimeSpan window, double factor)
        {
            var ticks = (long)Math.Max(TimeSpan.FromHours(1).Ticks, window.Ticks * factor);
            return TimeSpan.FromTicks(ticks);
        }
    }
}
