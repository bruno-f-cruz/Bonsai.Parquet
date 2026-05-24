using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using Bonsai;
using ParquetSharp;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Abstract source operator that reads a Parquet file and emits one <typeparamref name="TRow"/>
    /// per row. Subclass to declare which columns to read and how to assemble rows from per-column
    /// typed arrays.
    /// </summary>
    [Combinator(MethodName = nameof(Generate))]
    [WorkflowElementCategory(ElementCategory.Source)]
    public abstract class ParquetReader<TRow>
    {
        /// <summary>
        /// Gets or sets the path of the input Parquet file.
        /// </summary>
        [Description("The path of the input Parquet file.")]
        [Editor("Bonsai.Design.OpenFileNameEditor, Bonsai.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? FileName { get; set; }

        /// <summary>
        /// Gets or sets the number of rows decoded per internal batch.
        /// This is a decode-throughput knob only; output is always one <c>OnNext</c> per row.
        /// </summary>
        [Description("Number of rows decoded per batch. Decode-throughput knob only; output is always one OnNext per row.")]
        [DefaultValue(4096)]
        public int BatchSize { get; set; } = 4096;

        /// <summary>
        /// Gets the column bindings that declare which columns to read and their expected C# types.
        /// </summary>
        protected abstract IReadOnlyList<ColumnBinding> Columns { get; }

        /// <summary>
        /// Called once per decoded batch. Pull the typed column arrays you need from
        /// <paramref name="batch"/> into local variables, then return a closure that
        /// constructs one <typeparamref name="TRow"/> from the row index. The base class
        /// owns the per-row loop and emission.
        /// </summary>
        /// <param name="batch">
        /// The decoded batch. Buffers are framework-owned and reused; the returned factory
        /// is only invoked before the next call to <see cref="CreateRowFactory"/>, so it is
        /// safe to close over the column arrays.
        /// </param>
        /// <returns>A factory mapping row index → constructed row.</returns>
        protected abstract Func<int, TRow> CreateRowFactory(IParquetBatch batch);

        /// <summary>
        /// Opens the Parquet file and emits one <typeparamref name="TRow"/> per row.
        /// </summary>
        public IObservable<TRow> Generate()
        {
            return Observable.Create<TRow>(observer =>
            {
                var fileName = FileName;
                if (string.IsNullOrEmpty(fileName))
                    throw new InvalidOperationException("A valid file path must be specified.");
                if (!File.Exists(fileName))
                    throw new FileNotFoundException($"Parquet file not found: '{fileName}'.", fileName);

                var batchSize = BatchSize;
                if (batchSize <= 0)
                    throw new InvalidOperationException("BatchSize must be greater than zero.");

                var bindings = Columns?.ToList();
                if (bindings == null || bindings.Count == 0)
                    throw new InvalidOperationException("Columns must contain at least one ColumnBinding.");

                var namesSeen = new HashSet<string>(StringComparer.Ordinal);
                foreach (var b in bindings)
                {
                    if (!namesSeen.Add(b.Name))
                        throw new InvalidOperationException($"Duplicate column name declared: '{b.Name}'.");
                }

                var cts = new CancellationTokenSource();

                Task.Run(() =>
                {
                    try
                    {
                        using var fileReader = new ParquetFileReader(fileName);
                        var schema = fileReader.FileMetaData.Schema;

                        // Use ColumnRoot(i) to get the top-level field name for each leaf column.
                        // This correctly handles list (T[]) columns where the leaf is named "item"
                        // but the top-level field carries the user-visible name.
                        var fileColumnMap = new Dictionary<string, int>(schema.NumColumns, StringComparer.Ordinal);
                        for (int i = 0; i < schema.NumColumns; i++)
                            fileColumnMap[schema.ColumnRoot(i).Name] = i;

                        var columnIndices = new int[bindings.Count];
                        var buffers = new Array[bindings.Count];

                        for (int i = 0; i < bindings.Count; i++)
                        {
                            var binding = bindings[i];

                            if (!fileColumnMap.TryGetValue(binding.Name, out int colIdx))
                            {
                                var available = string.Join(", ", fileColumnMap.Keys);
                                throw new InvalidOperationException(
                                    $"Column '{binding.Name}' not found in file. Available columns: {available}");
                            }

                            columnIndices[i] = colIdx;
                            buffers[i] = Array.CreateInstance(binding.LogicalType, batchSize);
                        }

                        // Type validation: open row group 0 briefly to discover the actual C# type
                        // that ParquetSharp uses for each column (non-generic LogicalReader reflects it).
                        // Skipped for empty files (no rows → no type mismatch can occur at runtime).
                        if (fileReader.FileMetaData.NumRowGroups > 0)
                        {
                            using var validationRg = fileReader.RowGroup(0);
                            for (int i = 0; i < bindings.Count; i++)
                            {
                                using var untypedReader = validationRg.Column(columnIndices[i]).LogicalReader();
                                var readerType = untypedReader.GetType();
                                var actualType = readerType.IsGenericType
                                    ? readerType.GetGenericArguments()[0]
                                    : null;

                                if (actualType != bindings[i].LogicalType)
                                {
                                    throw new InvalidOperationException(
                                        $"Column '{bindings[i].Name}': expected type " +
                                        $"'{bindings[i].LogicalType.FullName}', but file has type " +
                                        $"'{actualType?.FullName ?? "unknown"}'.");
                                }
                            }
                        }

                        // Build per-column decoder factories (MakeGenericMethod once per column).
                        // Each factory opens a typed LogicalColumnReader for one row group and
                        // returns a ColumnDecoder that reads batches into the pre-allocated buffer.
                        var decoderFactories = new Func<RowGroupReader, ColumnDecoder>[bindings.Count];
                        var openMethod = typeof(ColumnDecoder)
                            .GetMethod(nameof(ColumnDecoder.Open), BindingFlags.Static | BindingFlags.NonPublic)!;

                        for (int i = 0; i < bindings.Count; i++)
                        {
                            var openGeneric = openMethod.MakeGenericMethod(bindings[i].LogicalType);
                            var capturedBuf = buffers[i];
                            var capturedIdx = columnIndices[i];
                            decoderFactories[i] = rg => (ColumnDecoder)openGeneric.Invoke(
                                null, new object[] { rg, capturedIdx, capturedBuf })!;
                        }

                        var batch = new ParquetBatch(
                            bindings.Select(b => b.Name).ToArray(),
                            buffers);

                        int numRowGroups = fileReader.FileMetaData.NumRowGroups;

                        for (int rg = 0; rg < numRowGroups; rg++)
                        {
                            if (cts.IsCancellationRequested) return;

                            using var rowGroupReader = fileReader.RowGroup(rg);
                            long numRowsInGroup = rowGroupReader.MetaData.NumRows;

                            // Open one typed column reader per column for this row group.
                            // Each reader maintains its read-position across batches within the group.
                            var decoders = new ColumnDecoder[bindings.Count];
                            try
                            {
                                for (int c = 0; c < bindings.Count; c++)
                                    decoders[c] = decoderFactories[c](rowGroupReader);

                                long rowsRead = 0;
                                while (rowsRead < numRowsInGroup)
                                {
                                    if (cts.IsCancellationRequested) return;

                                    int count = (int)Math.Min(batchSize, numRowsInGroup - rowsRead);

                                    for (int c = 0; c < decoders.Length; c++)
                                        decoders[c].Read(count);

                                    batch.SetRowCount(count);
                                    var factory = CreateRowFactory(batch);
                                    for (int r = 0; r < count; r++)
                                    {
                                        if (cts.IsCancellationRequested) return;
                                        observer.OnNext(factory(r));
                                    }

                                    rowsRead += count;
                                }
                            }
                            finally
                            {
                                for (int c = 0; c < decoders.Length; c++)
                                    decoders[c]?.Dispose();
                            }
                        }

                        if (!cts.IsCancellationRequested)
                            observer.OnCompleted();
                    }
                    catch (Exception ex)
                    {
                        if (!cts.IsCancellationRequested)
                            observer.OnError(ex);
                    }
                });

                return Disposable.Create(() => cts.Cancel());
            });
        }
    }

    /// <summary>
    /// Internal batch view passed to <see cref="ParquetReader{TRow}.ReadBatch"/>.
    /// Holds a dictionary of pre-allocated, framework-owned column buffers.
    /// </summary>
    internal sealed class ParquetBatch : IParquetBatch
    {
        readonly Dictionary<string, Array> _buffers;
        int _rowCount;

        internal ParquetBatch(string[] names, Array[] arrays)
        {
            _buffers = new Dictionary<string, Array>(names.Length, StringComparer.Ordinal);
            for (int i = 0; i < names.Length; i++)
                _buffers[names[i]] = arrays[i];
        }

        internal void SetRowCount(int rowCount) => _rowCount = rowCount;

        public int RowCount => _rowCount;

        public T[] Column<T>(string name)
        {
            if (!_buffers.TryGetValue(name, out var arr))
                throw new KeyNotFoundException($"Column '{name}' is not declared in this reader's Columns list.");
            return (T[])arr;
        }
    }

    /// <summary>
    /// Wraps a typed <see cref="LogicalColumnReader{TElement}"/> opened for one row group.
    /// Instances are created per row group and disposed at row group end.
    /// The generic <see cref="Open{T}"/> factory is resolved once at subscribe time via
    /// <c>MakeGenericMethod</c>, eliminating per-batch reflection overhead.
    /// </summary>
    internal abstract class ColumnDecoder : IDisposable
    {
        public abstract void Read(int count);
        public abstract void Dispose();

        internal static ColumnDecoder Open<T>(RowGroupReader rowGroup, int columnIndex, T[] buffer)
        {
            var reader = rowGroup.Column(columnIndex).LogicalReader<T>();
            return new TypedDecoder<T>(reader, buffer);
        }

        private sealed class TypedDecoder<T> : ColumnDecoder
        {
            readonly LogicalColumnReader<T> _reader;
            readonly T[] _buffer;

            internal TypedDecoder(LogicalColumnReader<T> reader, T[] buffer)
            {
                _reader = reader;
                _buffer = buffer;
            }

            public override void Read(int count) => _reader.ReadBatch(_buffer, 0, count);

            public override void Dispose() => _reader.Dispose();
        }
    }
}
