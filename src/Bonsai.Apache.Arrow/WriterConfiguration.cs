using System;
using System.IO;
using Bonsai.IO;

namespace Bonsai.Apache.Arrow
{
    // An immutable snapshot: property edits cannot change an active subscription.
    internal sealed class WriterConfiguration
    {
        internal readonly string Path;
        internal readonly string? Selector;
        internal readonly int BatchSize;
        internal readonly bool Overwrite, Buffered;
        internal readonly int QueueCapacity;
        internal readonly TimeSpan FlushInterval;

        internal WriterConfiguration(WriterOptions options)
        {
            if (string.IsNullOrEmpty(options.FileName)) throw new InvalidOperationException("A valid file path must be specified.");
            if (options.BatchSize <= 0) throw new InvalidOperationException("BatchSize must be greater than zero.");
            if (options.QueueCapacity <= 0) throw new InvalidOperationException("QueueCapacity must be greater than zero.");
            if (options.FlushInterval < TimeSpan.Zero ||
                (options.FlushInterval > TimeSpan.Zero && options.FlushInterval.TotalMilliseconds < 1) ||
                options.FlushInterval.TotalMilliseconds > uint.MaxValue - 1)
                throw new InvalidOperationException("FlushInterval must be zero or between 1 and 4294967294 milliseconds.");
            Path = PathHelper.AppendSuffix(options.FileName, options.Suffix);
            if (!options.Overwrite && File.Exists(Path)) throw new IOException($"The file '{Path}' already exists.");
            Selector = options.Selector;
            BatchSize = options.BatchSize;
            Overwrite = options.Overwrite;
            Buffered = options.Buffered;
            QueueCapacity = options.QueueCapacity;
            FlushInterval = options.FlushInterval;
        }
    }
}
