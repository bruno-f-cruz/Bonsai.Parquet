using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bonsai.Apache.Common;
using Bonsai.Expressions;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    /// <summary>Reads a Parquet file as scalar values or tuples according to an editable column schema.</summary>
    [WorkflowElementCategory(ElementCategory.Source)]
    [Description("Reads typed rows from an Apache Parquet file.")]
    public class ParquetReader : ExpressionBuilder
    {
        /// <inheritdoc/>
        public override Range<int> ArgumentRange => new Range<int>(0, 0);

        /// <summary>Gets or sets the input file path.</summary>
        [Description("The path of the input file.")]
        [Editor("Bonsai.Design.OpenFileNameEditor, Bonsai.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? FileName { get; set; }

        /// <summary>Gets or sets the maximum rows decoded at a time.</summary>
        [Description("The maximum rows decoded at a time. Output is one value per row.")]
        [DefaultValue(4096)]
        public int BatchSize { get; set; } = 4096;

        /// <summary>Gets or sets the columns to read and their expected types.</summary>
        [Description("The columns to read and their expected types.")]
        [Editor("Bonsai.Apache.Parquet.Design.ParquetSchemaEditor, Bonsai.Apache.Parquet.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public Collection<ColumnDefinition> Columns { get; set; } = new Collection<ColumnDefinition>();

        /// <inheritdoc/>
        public override Expression Build(IEnumerable<Expression> arguments)
        {
            var columns = Columns?.ToArray() ?? Array.Empty<ColumnDefinition>();
            if (columns.Length == 0) throw new InvalidOperationException("ParquetReader requires at least one column.");
            var type = RowFactory.GetOutputType(columns.Select(c => c.GetSystemType()).ToArray());
            return Expression.Call(Expression.Constant(this), typeof(ParquetReader).GetMethod(nameof(Generate))!.MakeGenericMethod(type));
        }

        /// <summary>Reads the configured columns as rows of the requested scalar or tuple type.</summary>
        public IObservable<TRow> Generate<TRow>() => Observable.Create<TRow>((observer, cancellation) => Task.Run(() =>
        {
            var fileName = FileName;
            var batchSize = BatchSize;
            var columns = Columns?.ToArray() ?? Array.Empty<ColumnDefinition>();
            if (string.IsNullOrEmpty(fileName)) throw new InvalidOperationException("A valid file path must be specified.");
            if (!File.Exists(fileName)) throw new FileNotFoundException($"File not found: {fileName}", fileName);
            if (batchSize <= 0) throw new InvalidOperationException("BatchSize must be greater than zero.");
            if (columns.Length == 0) throw new InvalidOperationException("Columns must contain at least one entry.");
            if (columns.Select(c => c.Name).Distinct(StringComparer.Ordinal).Count() != columns.Length)
                throw new InvalidOperationException("Column names must be unique.");
            var types = columns.Select(c => c.GetSystemType()).ToArray();
            var factory = RowFactory.Create<TRow>(types);
            var buffers = types.Select(t => Array.CreateInstance(t, batchSize)).ToArray();
            using (var fileReader = new ParquetFileReader(fileName))
            {
                var schema = fileReader.FileMetaData.Schema;

                // List columns use the root field name.
                var fileColumnMap = new Dictionary<string, int>(schema.NumColumns, StringComparer.Ordinal);
                for (int i = 0; i < schema.NumColumns; i++)
                    fileColumnMap[schema.ColumnRoot(i).Name] = i;

                var columnIndices = new int[columns.Length];

                for (int i = 0; i < columns.Length; i++)
                {
                    if (!fileColumnMap.TryGetValue(columns[i].Name, out int colIdx))
                    {
                        var available = string.Join(", ", fileColumnMap.Keys);
                        throw new InvalidOperationException(
                            $"Column '{columns[i].Name}' not found in file. Available: {available}");
                    }
                    columnIndices[i] = colIdx;
                }

                for (int i = 0; i < columns.Length; i++)
                {
                    var descriptor = schema.Column(columnIndices[i]);
                    var (_, _, actual) = descriptor.GetSystemTypes(LogicalTypeFactory.Default, null);
                    if (actual != types[i])
                        throw new InvalidOperationException(
                            $"Column '{columns[i].Name}': expected '{types[i].FullName}', file has '{actual?.FullName ?? "unknown"}'.");
                }

                // MakeGenericMethod once per column, not per batch.
                var openMethod = typeof(ColumnDecoder)
                    .GetMethod("Open", BindingFlags.Static | BindingFlags.NonPublic)!;
                var decoderFactories = new Func<RowGroupReader, ColumnDecoder>[columns.Length];
                for (int i = 0; i < columns.Length; i++)
                {
                    var openGeneric  = openMethod.MakeGenericMethod(types[i]);
                    var capturedBuf  = buffers[i];
                    var capturedIdx  = columnIndices[i];
                    decoderFactories[i] = rg =>
                        (ColumnDecoder)openGeneric.Invoke(null, new object[] { rg, capturedIdx, capturedBuf })!;
                }

                int numRowGroups = fileReader.FileMetaData.NumRowGroups;
                for (int rg = 0; rg < numRowGroups; rg++)
                {
                    if (cancellation.IsCancellationRequested) return;

                    using var rowGroupReader = fileReader.RowGroup(rg);
                    long numRowsInGroup = rowGroupReader.MetaData.NumRows;

                    var decoders = new ColumnDecoder[columns.Length];
                    try
                    {
                        for (int c = 0; c < columns.Length; c++)
                            decoders[c] = decoderFactories[c](rowGroupReader);

                        long rowsRead = 0;
                        while (rowsRead < numRowsInGroup)
                        {
                            if (cancellation.IsCancellationRequested) return;

                            int count = (int)Math.Min(batchSize, numRowsInGroup - rowsRead);
                            for (int c = 0; c < decoders.Length; c++)
                                decoders[c].Read(count);

                            for (int r = 0; r < count; r++)
                            {
                                if (cancellation.IsCancellationRequested) return;
                                observer.OnNext(factory(buffers, r));
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
            }
            if (!cancellation.IsCancellationRequested) observer.OnCompleted();
        }, cancellation));
    }
}
