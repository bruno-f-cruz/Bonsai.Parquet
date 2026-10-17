using System;
using System.IO;
using global::Apache.Arrow;
using global::Apache.Arrow.Compression;
using global::Apache.Arrow.Ipc;

namespace Bonsai.Apache.Arrow
{
    // Owns only the file resources; callers retain ownership of batches passed to Write.
    internal sealed class ArrowFileWriter : IDisposable
    {
        readonly FileStream stream;
        readonly global::Apache.Arrow.Ipc.ArrowFileWriter writer;
        bool disposed;

        internal ArrowFileWriter(string filename, Schema schema, bool enableCompression, bool overwrite = true)
        {
            stream = new FileStream(filename, overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write);
            try
            {
                var options = enableCompression ? new IpcOptions
                {
                    CompressionCodec = CompressionCodecType.Zstd,
                    CompressionCodecFactory = new CompressionCodecFactory()
                } : null;
                writer = new global::Apache.Arrow.Ipc.ArrowFileWriter(stream, schema, leaveOpen: true, options);
                try { writer.WriteStart(); }
                catch { writer.Dispose(); throw; }
            }
            catch { stream.Dispose(); throw; }
        }

        internal void Write(RecordBatch batch) => writer.WriteRecordBatch(batch);
        internal void Flush() => stream.Flush();

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            try { writer.WriteEnd(); }
            finally
            {
                try { writer.Dispose(); }
                finally { stream.Dispose(); }
            }
        }
    }
}
