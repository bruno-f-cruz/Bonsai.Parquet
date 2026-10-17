using System;
using System.Collections.Generic;
using System.Linq;
using global::Apache.Arrow;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Arrow
{
    /// <summary>Buffers items and writes owned batches. Calls must be serialized by the caller.</summary>
    /// <typeparam name="T">The source item type.</typeparam>
    public sealed class ArrowBatchWriter<T> : IDisposable
    {
        readonly ArrowFileWriter writer;
        readonly Schema schema;
        readonly int bufferSize;
        readonly Func<IList<T>, Schema, RecordBatch>? createRecordBatch;
        readonly ColumnBuffer<T>[] buffers = System.Array.Empty<ColumnBuffer<T>>();
        readonly List<T>? items;
        int bufferedRows;
        bool failed, disposed;

        /// <summary>Creates a writer with columns inferred from the input type.</summary>
        public ArrowBatchWriter(string filename, int bufferSize, bool enableCompression = false,
            bool overwrite = true, string? selector = null)
            : this(filename, MemberSchema.Infer(typeof(T), selector), bufferSize, enableCompression, overwrite) { }

        internal ArrowBatchWriter(string filename, MemberColumn[] members, int bufferSize, bool enableCompression, bool overwrite)
        {
            if (bufferSize <= 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));
            this.bufferSize = bufferSize;
            schema = SchemaInference.GetSchema(members);
            buffers = members.Select(member => ColumnBuffer<T>.Create(member, bufferSize)).ToArray();
            writer = new ArrowFileWriter(filename, schema, enableCompression, overwrite);
        }

        /// <summary>Creates a writer which flushes on size, explicit Flush, or disposal.</summary>
        /// <param name="filename">The output file path.</param>
        /// <param name="schema">The schema used by every batch.</param>
        /// <param name="bufferSize">The maximum buffered input items.</param>
        /// <param name="createRecordBatch">Creates an owned batch, disposed after writing.</param>
        /// <param name="enableCompression">Enables Zstandard compression.</param>
        /// <param name="overwrite">Allows replacing an existing file.</param>
        public ArrowBatchWriter(string filename, Schema schema, int bufferSize,
            Func<IList<T>, Schema, RecordBatch> createRecordBatch, bool enableCompression = false, bool overwrite = true)
        {
            if (bufferSize <= 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.createRecordBatch = createRecordBatch ?? throw new ArgumentNullException(nameof(createRecordBatch));
            this.bufferSize = bufferSize;
            items = new List<T>();
            writer = new ArrowFileWriter(filename, schema, enableCompression, overwrite);
        }

        /// <summary>Adds an item and flushes when the batch is full.</summary>
        public void Write(T item)
        {
            if (disposed) throw new ObjectDisposedException(GetType().Name);
            try
            {
                if (items != null) items.Add(item);
                else foreach (var buffer in buffers) buffer.Append(item);
            }
            catch
            {
                foreach (var buffer in buffers) buffer.Truncate(bufferedRows);
                throw;
            }
            bufferedRows++;
            if (bufferedRows >= bufferSize) Flush();
        }

        /// <summary>Writes all pending items and flushes the file stream.</summary>
        public void Flush()
        {
            if (disposed) throw new ObjectDisposedException(GetType().Name);
            try
            {
                if (bufferedRows > 0)
                {
                    using var batch = createRecordBatch != null ? createRecordBatch(items!, schema) : CreateBatch();
                    writer.Write(batch);
                    items?.Clear();
                    foreach (var buffer in buffers) buffer.Truncate(0);
                    bufferedRows = 0;
                }
                writer.Flush();
            }
            catch { failed = true; throw; }
        }

        RecordBatch CreateBatch()
        {
            var arrays = new IArrowArray[buffers.Length];
            try
            {
                for (int i = 0; i < buffers.Length; i++) arrays[i] = buffers[i].Build();
                return new RecordBatch(schema, arrays, bufferedRows);
            }
            catch
            {
                foreach (var array in arrays) array?.Dispose();
                throw;
            }
        }

        /// <summary>Writes the final partial batch and footer, then closes the file.</summary>
        public void Dispose()
        {
            if (disposed) return;
            try { if (!failed) Flush(); }
            finally
            {
                disposed = true;
                items?.Clear();
                foreach (var buffer in buffers) buffer.Truncate(0);
                writer.Dispose();
            }
        }
    }
}
