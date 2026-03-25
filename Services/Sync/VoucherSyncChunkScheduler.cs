using System;

namespace Acczite20.Services.Sync
{
    public sealed class VoucherSyncChunkScheduler
    {
        private TimeSpan _window;
        
        // Strict Bounds for Layer 1
        private readonly TimeSpan _minWindow = TimeSpan.FromHours(1);
        private readonly TimeSpan _maxWindow = TimeSpan.FromDays(30);
        
        // Layer 2 Constants
        private const int _layer2BatchSize = 25;
        private const int _layer1CoarseLimit = 500;

        private bool _isSafeModeActive = false;
        private int _stableChunksCount = 0;

        public VoucherSyncChunkScheduler(TimeSpan? initialWindow = null)
        {
            _window = initialWindow ?? TimeSpan.FromDays(1);
            Clamp();
        }

        public TimeSpan CurrentWindow => _window;
        public TimeSpan MinWindow => _minWindow;
        
        // The hard cap for Pass 2 inside the executor
        public int MaxVouchersPerChunk => _layer2BatchSize;
        
        // The density threshold for Pass 1
        public int CoarseDensityLimit => _layer1CoarseLimit;

        public void Adjust(TimeSpan elapsed, int voucherCount, long bytesReceived, bool failed, bool isSafeMode)
        {
            if (_isSafeModeActive || isSafeMode)
            {
                ActivateSafeMode();
                return;
            }

            if (failed)
            {
                // Halve the window if chunk overloaded OR Tally timed out
                _stableChunksCount = 0;
                ScaleWindow(0.5);
                return;
            }

            // Grow window slowly if Tally passes easily (Layer 1 returned headers effortlessly)
            if (voucherCount < (_layer1CoarseLimit / 2))
            {
                _stableChunksCount++;
                if (_stableChunksCount >= 3)
                {
                    ScaleWindow(1.5);
                    _stableChunksCount = 0;
                }
            }
        }

        private void ScaleWindow(double factor)
        {
            double ticks = _window.Ticks * factor;
            long maxTicks = _maxWindow.Ticks;
            long minTicks = _minWindow.Ticks;
            
            ticks = Math.Max(minTicks, Math.Min(maxTicks, ticks));
            _window = TimeSpan.FromTicks((long)ticks);
        }

        private void Clamp()
        {
            if (_window < _minWindow) _window = _minWindow;
            if (_window > _maxWindow) _window = _maxWindow;
        }

        public void ActivateSafeMode()
        {
            _isSafeModeActive = true;
            _window = _minWindow;
            _stableChunksCount = 0;
        }

        public void ReduceVoucherCapAndWindow()
        {
            // Fallback for controller retries
            ScaleWindow(0.5);
        }
    }
}
