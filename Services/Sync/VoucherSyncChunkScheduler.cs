using System;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherSyncChunkScheduler
    {
        private TimeSpan _window;

        // ── Bounds ─────────────────────────────────────────────────────────────────
        // MinWindow = 1 hour — 15 min caused oscillation on sparse datasets (e.g. 1 voucher/day):
        //   the window would shrink past the point of diminishing returns, bouncing between
        //   1-voucher chunks and re-growth indefinitely. 1 hour is the practical floor.
        // MaxWindow = 30 days — critical for historical periods with zero data:
        //   empty chunks grow to 30 days quickly, so scanning years of history
        //   takes dozens of requests instead of thousands.
        //   When the window reaches dense data it shrinks automatically via the overload check.
        private readonly TimeSpan _minWindow = TimeSpan.FromHours(1);
        private readonly TimeSpan _maxWindow = TimeSpan.FromDays(30);

        // ── Layer 1: Coarse density guard ──────────────────────────────────────────
        // If Pass 1 (headers only) returns more than this many vouchers, the window
        // is too large. Shrink it before firing the heavy detail pass.
        // 50 is conservative — even 50 × 20 ledger entries = 1000 rows, which Tally
        // can handle easily per request.
        private const int DefaultCoarseDensityLimit = 50;
        private int _coarseDensityLimit;

        // ── Layer 2: ID-batch size ─────────────────────────────────────────────────
        // After Layer 1 passes, vouchers are sent to Tally in batches of this size.
        // User-controlled via the BatchSize UI control.
        private int _layer2BatchSize;

        private bool _isSafeModeActive;
        private int _stableChunksCount;

        /// <param name="initialWindow">Starting window. Defaults to 1 hour (conservative).</param>
        /// <param name="layer2BatchSize">Vouchers per ID-batch in Pass 2. Defaults to 25.</param>
        /// <param name="coarseDensityLimit">Max vouchers per window before shrinking. Defaults to 50.</param>
        public VoucherSyncChunkScheduler(
            TimeSpan? initialWindow      = null,
            int       layer2BatchSize    = 25,
            int       coarseDensityLimit = DefaultCoarseDensityLimit)
        {
            _window             = initialWindow ?? TimeSpan.FromHours(1);
            _layer2BatchSize    = Math.Max(1, layer2BatchSize);
            _coarseDensityLimit = Math.Max(10, coarseDensityLimit);
            Clamp();
        }

        public TimeSpan CurrentWindow    => _window;
        public TimeSpan MinWindow        => _minWindow;
        public int MaxVouchersPerChunk   => _layer2BatchSize;
        public int CoarseDensityLimit    => _coarseDensityLimit;

        public void Adjust(TimeSpan elapsed, int voucherCount, long bytesReceived, bool failed, bool isSafeMode)
        {
            if (_isSafeModeActive || isSafeMode)
            {
                // Safe mode: lock at MinWindow, never grow
                _window = _minWindow;
                return;
            }

            if (failed)
            {
                _stableChunksCount = 0;
                ScaleWindow(0.5);
                return;
            }

            // Grow window slowly only when comfortably under the density limit
            if (voucherCount < _coarseDensityLimit / 2)
            {
                _stableChunksCount++;
                if (_stableChunksCount >= 3)
                {
                    ScaleWindow(2.0);
                    _stableChunksCount = 0;
                }
            }
            else
            {
                // Near the density limit — hold steady, don't grow
                _stableChunksCount = 0;
            }
        }

        public void ActivateSafeMode()
        {
            _isSafeModeActive  = true;
            _window            = _minWindow;
            _stableChunksCount = 0;
        }

        public void ReduceVoucherCapAndWindow()
        {
            ScaleWindow(0.5);
        }

        private void ScaleWindow(double factor)
        {
            var ticks = Math.Max(_minWindow.Ticks,
                            Math.Min(_maxWindow.Ticks, (long)(_window.Ticks * factor)));
            _window = TimeSpan.FromTicks(ticks);
        }

        private void Clamp()
        {
            if (_window < _minWindow) _window = _minWindow;
            if (_window > _maxWindow) _window = _maxWindow;
        }
    }
}
