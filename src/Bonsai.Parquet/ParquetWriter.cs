using System;
using System.ComponentModel;
using System.IO;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using Bonsai;
using Bonsai.IO;
using ParquetSharp;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Represents an operator that writes each element of the sequence as a row in a Parquet file.
    /// </summary>
    [Combinator]
    [DefaultProperty(nameof(FileName))]
    [WorkflowElementCategory(ElementCategory.Sink)]
    [Description("Writes each element of the sequence as a row in a Parquet file.")]
    public class ParquetWriter
    {
        /// <summary>
        /// Gets or sets the path of the output Parquet file.
        /// </summary>
        [Description("The path of the output Parquet file.")]
        [Editor("Bonsai.Design.SaveFileNameEditor, Bonsai.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? FileName { get; set; }

        /// <summary>
        /// Gets or sets the suffix appended to the file name to generate unique file names.
        /// </summary>
        [Description("The suffix appended to the file name to generate unique file names.")]
        public PathSuffix Suffix { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the output file should be overwritten if it already exists.
        /// </summary>
        [Description("Indicates whether the output file should be overwritten if it already exists.")]
        public bool Overwrite { get; set; }

        /// <summary>
        /// Gets or sets the inner properties that will be selected when writing each element of the sequence.
        /// </summary>
        [Description("The inner properties that will be selected when writing each element of the sequence.")]
        public string? Selector { get; set; }

        /// <summary>
        /// Gets or sets the compression codec used to compress each row group.
        /// </summary>
        [Description("The compression codec used to compress each row group.")]
        [DefaultValue(Compression.Snappy)]
        public Compression CompressionMethod { get; set; } = Compression.Snappy;

        /// <summary>
        /// Gets or sets the number of rows buffered before a row group is flushed to disk.
        /// </summary>
        [Description("The number of rows buffered before a row group is flushed to disk.")]
        [DefaultValue(100_000)]
        public int RowGroupSize { get; set; } = 100_000;

        /// <summary>
        /// Writes each element of the source sequence to a Parquet file, passing the sequence through unchanged.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
        /// <param name="source">The source sequence whose elements to write.</param>
        /// <returns>An observable sequence identical to <paramref name="source"/>.</returns>
        public IObservable<TSource> Process<TSource>(IObservable<TSource> source)
        {
            return Observable.Create<TSource>(observer =>
            {
                var fileName = FileName;
                if (string.IsNullOrEmpty(fileName))
                {
                    throw new InvalidOperationException("A valid file path must be specified.");
                }

                PathHelper.EnsureDirectory(fileName);
                fileName = PathHelper.AppendSuffix(fileName, Suffix);
                if (File.Exists(fileName) && !Overwrite)
                {
                    throw new IOException($"The file '{fileName}' already exists.");
                }

                var rowType = typeof(TSource);
                // Validate schema at subscribe time so unsupported types fail early.
                var columns = SchemaInference.InferColumns(rowType);
                _ = columns; // validation only; ParquetSink rebuilds internally

                var sink = new ParquetSink<TSource>(fileName, CompressionMethod, RowGroupSize);

                var subscription = source.Do(item =>
                {
                    sink.AppendRow(item);
                }).SubscribeSafe(observer);

                return new CompositeDisposable(subscription, sink);
            });
        }
    }
}
