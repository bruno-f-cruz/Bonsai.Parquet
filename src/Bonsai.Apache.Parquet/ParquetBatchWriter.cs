using System;
using System.IO;
using System.Collections.Generic;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    /// <summary>Buffers items and writes row groups. Calls must be serialized by the caller.</summary>
    /// <typeparam name="T">The source item type.</typeparam>
    public sealed class ParquetBatchWriter<T> : IDisposable
    {
        readonly ParquetFileWriter fileWriter;
        readonly FileStream stream;
        readonly int bufferSize;
        readonly ColumnBuffer<T>[] buffers = Array.Empty<ColumnBuffer<T>>();
        readonly List<T>? items;
        readonly Action<IList<T>, RowGroupWriter>? writeBatch;
        int bufferedRows;
        bool disposed;
        bool failed;

        /// <summary>Creates a writer with columns inferred from the input type.</summary>
        public ParquetBatchWriter(string filename, int bufferSize, Compression compression = Compression.Snappy,
            bool overwrite = true, string? selector = null)
            : this(filename, SchemaInference.BuildPlan(typeof(T), selector), compression, bufferSize, overwrite) { }

        /// <summary>Creates a writer with explicit columns and a callback that writes each buffered batch.</summary>
        public ParquetBatchWriter(string filename, Column[] columns, int bufferSize,
            Action<IList<T>, RowGroupWriter> writeBatch, Compression compression = Compression.Snappy, bool overwrite = true)
        {
            if (bufferSize <= 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));
            if (columns == null) throw new ArgumentNullException(nameof(columns));
            this.writeBatch = writeBatch ?? throw new ArgumentNullException(nameof(writeBatch));
            this.bufferSize = bufferSize;
            items = new List<T>();
            fileWriter = OpenFile(filename, columns, compression, overwrite, out stream);
        }

        internal ParquetBatchWriter(string path, ColumnPlan[] plan, Compression compression, int bufferSize, bool overwrite = true)
        {
            if (bufferSize <= 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));
            this.bufferSize = bufferSize;
            var columns = new Column[plan.Length];
            buffers = new ColumnBuffer<T>[plan.Length];
            for (int i = 0; i < plan.Length; i++)
            {
                columns[i] = plan[i].Column;
                buffers[i] = ColumnBuffer<T>.Create(plan[i], bufferSize);
            }
            fileWriter = OpenFile(path, columns, compression, overwrite, out stream);
        }

        static ParquetFileWriter OpenFile(string path, Column[] columns, Compression compression, bool overwrite, out FileStream stream)
        {
            stream = new FileStream(path, overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write);
            try { return new ParquetFileWriter(stream, columns, compression: compression, leaveOpen: true); }
            catch { stream.Dispose(); throw; }
        }

        /// <summary>Adds an item and flushes when the batch is full.</summary>
        public void Write(T row)
        {
            if (disposed) throw new ObjectDisposedException(GetType().Name);
            try
            {
                if (items != null) items.Add(row);
                else foreach (var buffer in buffers) buffer.Append(row, bufferedRows);
            }
            catch
            {
                // Discard a partially converted row; earlier rows remain flushable.
                foreach (var buffer in buffers) buffer.Clear(bufferedRows, 1);
                throw;
            }
            bufferedRows++;
            if (bufferedRows >= bufferSize) Flush();
        }

        /// <summary>Writes pending items and flushes the file stream.</summary>
        public void Flush()
        {
            if (disposed) throw new ObjectDisposedException(GetType().Name);
            try { FlushRowGroup(); stream.Flush(); }
            catch { failed = true; throw; }
        }

        void FlushRowGroup()
        {
            if (bufferedRows == 0) return;
            using var rowGroupWriter = fileWriter.AppendRowGroup();
            if (writeBatch != null) writeBatch(items!, rowGroupWriter);
            else foreach (var buffer in buffers)
            {
                using var column = rowGroupWriter.NextColumn();
                buffer.Write(column, bufferedRows);
            }
            rowGroupWriter.Close();
            items?.Clear();
            foreach (var buffer in buffers) buffer.Clear(0, bufferedRows);
            bufferedRows = 0;
        }

        /// <summary>Writes the final partial batch and footer, then closes the file.</summary>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            try
            {
                if (!failed)
                {
                    FlushRowGroup();
                    fileWriter.Close();
                }
            }
            finally
            {
                items?.Clear();
                foreach (var buffer in buffers) buffer.Clear(0, bufferedRows);
                try { fileWriter.Dispose(); }
                finally { stream.Dispose(); }
            }
        }
    }
}
