using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Acczite20.Services
{
    public sealed class TallyXmlStreamLease : IDisposable, IAsyncDisposable
    {
        private readonly Func<ValueTask> _disposeAsync;
        private int _disposed;

        internal TallyXmlStreamLease(Stream stream, long? declaredSize, Func<ValueTask> disposeAsync)
        {
            Stream = stream;
            DeclaredSize = declaredSize ?? 0;
            _disposeAsync = disposeAsync;
        }

        public Stream Stream { get; }

        public long DeclaredSize { get; }

        public void Dispose()
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            await _disposeAsync();
        }
    }
}
