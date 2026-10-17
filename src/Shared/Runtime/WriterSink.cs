using System;
using System.Threading;

namespace Bonsai.Apache.Common
{
    internal sealed class WriterSink<T> : IDisposable
    {
        readonly Action<T> write;
        readonly Action flush;
        Action? dispose;
        internal WriterSink(Action<T> write, Action flush, Action dispose)
        {
            this.write = write;
            this.flush = flush;
            this.dispose = dispose;
        }
        internal void Write(T item) => write(item);
        internal void Flush() => flush();
        public void Dispose() => Interlocked.Exchange(ref dispose, null)?.Invoke();
    }
}
