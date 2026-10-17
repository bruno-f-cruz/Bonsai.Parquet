using System;
using System.ComponentModel;
using System.Linq;
using System.Reactive.Linq;
using global::Apache.Arrow;
using Bonsai.Apache.Common;
using Bonsai.IO;

namespace Bonsai.Apache.Arrow
{
    /// <summary>Writes observable rows or existing record batches to an Arrow IPC file (Feather V2).</summary>
    [Combinator]
    [DefaultProperty(nameof(FileName))]
    [WorkflowElementCategory(ElementCategory.Sink)]
    [Description("Writes each element of the sequence to an Apache Arrow IPC file.")]
    public class ArrowWriter : WriterOptions
    {
        /// <summary>Gets or sets whether to compress batches using Zstandard.</summary>
        [DefaultValue(false)]
        [Description("Compress output using Zstandard.")]
        public bool EnableCompression { get; set; }

        /// <summary>Writes inferred columns and passes each input item through unchanged.</summary>
        public IObservable<TSource> Process<TSource>(IObservable<TSource> source)
        {
            return Observable.Defer(() =>
            {
                var options = new WriterConfiguration(this);
                var compression = EnableCompression;
                var members = MemberSchema.Infer(typeof(TSource), options.Selector);
                return WriterPipeline.Process(source, options.Buffered, options.QueueCapacity, options.FlushInterval, () =>
                {
                    PathHelper.EnsureDirectory(options.Path);
                    var writer = new ArrowBatchWriter<TSource>(options.Path, members, options.BatchSize,
                        compression, options.Overwrite);
                    return new WriterSink<TSource>(writer.Write, writer.Flush, writer.Dispose);
                });
            });
        }

        /// <summary>Writes caller-owned batches directly, preserving schema and buffer ownership.</summary>
        /// <remarks>An empty source creates no file because its schema is unknown. Selector and BatchSize are unused.
        /// Buffered writes retain a shared batch view until writing finishes; callers may dispose their own batch,
        /// but must not mutate its buffers while writing.</remarks>
        public IObservable<RecordBatch> Process(IObservable<RecordBatch> source)
        {
            return Observable.Defer(() =>
            {
                var options = new WriterConfiguration(this);
                var compression = EnableCompression;
                return WriterPipeline.Process(source, options.Buffered, options.QueueCapacity, options.FlushInterval, () =>
                {
                    var sink = new RecordBatchSink(options, compression);
                    return new WriterSink<RecordBatch>(sink.Write, sink.Flush, sink.Dispose);
                }, batch => batch?.SliceShared(0, batch.Length) ?? throw new ArgumentNullException(nameof(batch)),
                    batch => batch.Dispose());
            });
        }

        sealed class RecordBatchSink : IDisposable
        {
            readonly WriterConfiguration options;
            readonly bool compression;
            ArrowFileWriter? writer;
            Schema? schema;
            byte[]? serializedSchema;

            internal RecordBatchSink(WriterConfiguration options, bool compression)
            {
                this.options = options;
                this.compression = compression;
            }

            internal void Write(RecordBatch batch)
            {
                if (batch == null) throw new ArgumentNullException(nameof(batch));
                if (writer == null)
                {
                    schema = batch.Schema;
                    serializedSchema = SchemaComparison.Serialize(schema);
                    PathHelper.EnsureDirectory(options.Path);
                    writer = new ArrowFileWriter(options.Path, schema, compression, options.Overwrite);
                }
                else if (!ReferenceEquals(schema, batch.Schema) &&
                    !serializedSchema!.SequenceEqual(SchemaComparison.Serialize(batch.Schema)))
                    throw new InvalidOperationException("All record batches must have the same schema.");
                writer.Write(batch);
            }

            internal void Flush() => writer?.Flush();
            public void Dispose() => writer?.Dispose();
        }
    }
}
