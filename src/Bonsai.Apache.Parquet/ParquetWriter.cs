using System;
using System.ComponentModel;
using System.Reactive.Linq;
using Bonsai.Apache.Common;
using Bonsai.IO;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    /// <summary>Writes each element of the sequence as a row in a Parquet file.</summary>
    [Combinator]
    [DefaultProperty(nameof(FileName))]
    [WorkflowElementCategory(ElementCategory.Sink)]
    [Description("Writes each element of the sequence to an Apache Parquet file.")]
    public class ParquetWriter : WriterOptions
    {
        /// <summary>Gets or sets the compression codec applied to each row group.</summary>
        [Description("The compression codec applied to each row group.")]
        [DefaultValue(Compression.Snappy)]
        public Compression CompressionMethod { get; set; } = Compression.Snappy;

        /// <summary>Writes inferred columns and passes each input item through unchanged.</summary>
        public IObservable<TSource> Process<TSource>(IObservable<TSource> source)
        {
            return Observable.Defer(() =>
            {
                var options = new WriterConfiguration(this);
                var compression = CompressionMethod;
                var plan = SchemaInference.BuildPlan(typeof(TSource), options.Selector);
                return WriterPipeline.Process(source, options.Buffered, options.QueueCapacity, options.FlushInterval, () =>
                {
                    PathHelper.EnsureDirectory(options.Path);
                    var writer = new ParquetBatchWriter<TSource>(options.Path, plan,
                        compression, options.RowGroupSize, options.Overwrite);
                    return new WriterSink<TSource>(writer.Write, writer.Flush, writer.Dispose);
                });
            });
        }
    }
}
