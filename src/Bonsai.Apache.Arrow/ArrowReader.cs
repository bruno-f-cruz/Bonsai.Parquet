using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Bonsai.Apache.Common;
using Bonsai.Expressions;
using global::Apache.Arrow.Compression;

namespace Bonsai.Apache.Arrow
{
    /// <summary>Reads an Arrow file as scalar values or tuples according to an editable column schema.</summary>
    [WorkflowElementCategory(ElementCategory.Source)]
    [Description("Reads typed rows from an Apache Arrow file.")]
    public class ArrowReader : ExpressionBuilder
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
        [Editor("Bonsai.Apache.Arrow.Design.ArrowSchemaEditor, Bonsai.Apache.Arrow.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public Collection<ColumnDefinition> Columns { get; set; } = new Collection<ColumnDefinition>();

        /// <inheritdoc/>
        public override Expression Build(IEnumerable<Expression> arguments)
        {
            var columns = Columns?.ToArray() ?? Array.Empty<ColumnDefinition>();
            if (columns.Length == 0) throw new InvalidOperationException("ArrowReader requires at least one column.");
            var type = RowFactory.GetOutputType(columns.Select(c => c.GetSystemType()).ToArray());
            return Expression.Call(Expression.Constant(this), typeof(ArrowReader).GetMethod(nameof(Generate))!.MakeGenericMethod(type));
        }

        /// <summary>Reads the configured columns as rows of the requested scalar or tuple type.</summary>
        public IObservable<TRow> Generate<TRow>() => Observable.Create<TRow>((observer, cancellation) => Task.Run(async () =>
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
            var decoders = types.Select((type, i) => ColumnDecoder.Open(type, buffers[i])).ToArray();
            using (var stream = File.OpenRead(fileName))
            using (var reader = new global::Apache.Arrow.Ipc.ArrowFileReader(stream, new CompressionCodecFactory()))
            {
                var schema = await reader.GetSchema(cancellation).ConfigureAwait(false);
                var indices = columns.Select(c => schema.GetFieldIndex(c.Name)).ToArray();
                for (int i = 0; i < columns.Length; i++)
                {
                    if (indices[i] < 0) throw new InvalidOperationException($"Column '{columns[i].Name}' not found in file.");
                    if (!SchemaComparison.TypeEquals(schema.GetFieldByIndex(indices[i]).DataType, SchemaInference.GetArrowType(types[i])))
                        throw new InvalidOperationException($"Column '{columns[i].Name}' does not match the configured type.");
                }
                while (!cancellation.IsCancellationRequested)
                {
                    using var batch = await reader.ReadNextRecordBatchAsync(cancellation).ConfigureAwait(false);
                    if (batch == null) break;
                    for (int offset = 0; offset < batch.Length;)
                    {
                        var count = Math.Min(batchSize, batch.Length - offset);
                        for (int c = 0; c < decoders.Length; c++)
                            decoders[c].Read(batch.Column(indices[c]), offset, count, cancellation);
                        for (int r = 0; r < count && !cancellation.IsCancellationRequested; r++) observer.OnNext(factory(buffers, r));
                        offset += count;
                        if (cancellation.IsCancellationRequested) return;
                    }
                }
            }
            if (!cancellation.IsCancellationRequested) observer.OnCompleted();
        }, cancellation));
    }
}
