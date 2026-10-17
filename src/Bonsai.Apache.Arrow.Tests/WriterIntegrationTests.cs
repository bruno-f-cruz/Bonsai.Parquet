using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Xml;
using Bonsai.Expressions;
using global::Apache.Arrow;
using global::Apache.Arrow.Ipc;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class WriterIntegrationTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".data");

        [TestCleanup]
        public void Cleanup() { if (File.Exists(path)) File.Delete(path); }

        object CreateWriter(string format, bool buffered, int batchSize = 100, int flushMilliseconds = 0)
            => format == "Arrow" ? (object)new ArrowWriter
            {
                FileName = path, Buffered = buffered, BatchSize = batchSize,
                QueueCapacity = 2048,
                FlushInterval = TimeSpan.FromMilliseconds(flushMilliseconds)
            } : new Parquet.ParquetWriter
            {
                FileName = path, Buffered = buffered, BatchSize = batchSize,
                QueueCapacity = 2048,
                FlushInterval = TimeSpan.FromMilliseconds(flushMilliseconds)
            };

        static IObservable<T> Write<T>(object writer, IObservable<T> source) => writer is ArrowWriter arrow
            ? arrow.Process(source) : ((Parquet.ParquetWriter)writer).Process(source);

        int[] Read(string format)
        {
            if (format == "Arrow")
            {
                var reader = new ArrowReader { FileName = path };
                reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = ArrowColumnType.Int32 });
                return reader.Generate<int>().ToArray().Timeout(TimeSpan.FromSeconds(5)).Wait();
            }
            using var file = new ParquetSharp.ParquetFileReader(path);
            var values = new List<int>();
            for (int i = 0; i < file.FileMetaData.NumRowGroups; i++)
            {
                using var group = file.RowGroup(i);
                using var column = group.Column(0).LogicalReader<int>();
                values.AddRange(column.ReadAll((int)group.MetaData.NumRows));
            }
            return values.ToArray();
        }

        [TestMethod]
        [DataRow("Arrow", false)]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", false)]
        [DataRow("Parquet", true)]
        public void CompletionDrainsAndFinalizesBeforeTheFileIsRead(string format, bool buffered)
        {
            var expected = Enumerable.Range(0, 1500).ToArray();
            var writer = CreateWriter(format, buffered, batchSize: 512);
            var actual = Write(writer, expected.ToObservable()).ToArray().Timeout(TimeSpan.FromSeconds(10)).Wait();
            CollectionAssert.AreEqual(expected, actual);
            CollectionAssert.AreEqual(expected, Read(format));
        }

        sealed class MutableSample
        {
            public int Value { get; set; }
        }

        [TestMethod]
        [DataRow("Arrow", false)]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", false)]
        [DataRow("Parquet", true)]
        public void ProcessedNotificationMeansScalarPropertiesHaveBeenCaptured(string format, bool buffered)
        {
            using var source = new Subject<MutableSample>();
            using var processed = new ManualResetEventSlim();
            using var ended = new ManualResetEventSlim();
            Exception? failure = null;
            using var subscription = Write(CreateWriter(format, buffered), source)
                .Subscribe(_ => processed.Set(), error => { failure = error; ended.Set(); }, ended.Set);
            var row = new MutableSample { Value = 42 };
            source.OnNext(row);
            Assert.IsTrue(processed.Wait(TimeSpan.FromSeconds(5)));
            row.Value = 99;
            source.OnCompleted();
            Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsNull(failure);
            CollectionAssert.AreEqual(new[] { 42 }, Read(format));
        }

        [TestMethod]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", true)]
        public void FlushIntervalWritesAPartialBatchWhileTheSourceIsIdle(string format, bool buffered)
        {
            using var source = new Subject<int>();
            using var processed = new ManualResetEventSlim();
            using var ended = new ManualResetEventSlim();
            Exception? failure = null;
            var writer = CreateWriter(format, buffered, flushMilliseconds: 100);
            long initialLength = 0;
            using var subscription = Write(writer, source).Subscribe(value =>
            {
                if (value == 1) { initialLength = new FileInfo(path).Length; processed.Set(); }
            }, ex => { failure = ex; ended.Set(); }, ended.Set);
            source.OnNext(1);
            Assert.IsTrue(processed.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsTrue(SpinWait.SpinUntil(() => new FileInfo(path).Length > initialLength, TimeSpan.FromSeconds(5)),
                "The pending row must be flushed without a second input or source completion.");
            source.OnNext(2);
            source.OnCompleted();
            Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsNull(failure);
            CollectionAssert.AreEqual(new[] { 1, 2 }, Read(format));
            if (format == "Arrow")
            {
                using var file = new ArrowFileReader(File.OpenRead(path));
                Assert.AreEqual(2, file.RecordBatchCountAsync().GetAwaiter().GetResult());
            }
            else
            {
                using var file = new ParquetSharp.ParquetFileReader(path);
                Assert.AreEqual(2, file.FileMetaData.NumRowGroups);
            }
        }

        [TestMethod]
        [DataRow("Arrow", false)]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", false)]
        [DataRow("Parquet", true)]
        public void CancellationDrainsAcceptedItemsAndFinalizesTheFile(string format, bool buffered)
        {
            using var source = new Subject<int>();
            var writer = CreateWriter(format, buffered);
            var subscription = Write(writer, source).Subscribe();
            var expected = Enumerable.Range(0, 10).ToArray();
            foreach (var value in expected) source.OnNext(value);
            subscription.Dispose();
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                try { using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None); return true; }
                catch (IOException) { return false; }
            }, TimeSpan.FromSeconds(5)), "Cancellation drains and closes in the background.");
            CollectionAssert.AreEqual(expected, Read(format));
        }

        [TestMethod]
        [DataRow("Arrow", false)]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", false)]
        [DataRow("Parquet", true)]
        public void SourceErrorsPreserveAcceptedRowsAndFinalizeTheFile(string format, bool buffered)
        {
            var expected = Enumerable.Range(0, 10).ToArray();
            var error = new InvalidOperationException("source failed");
            var source = expected.ToObservable().Concat(Observable.Throw<int>(error));
            var observed = Assert.ThrowsExactly<InvalidOperationException>(() => Write(CreateWriter(format, buffered), source)
                .Timeout(TimeSpan.FromSeconds(5)).Wait());
            Assert.AreSame(error, observed);
            CollectionAssert.AreEqual(expected, Read(format));
        }

        [TestMethod]
        public void BufferedRecordBatchesRetainBuffersAfterTheCallerDisposesThem()
        {
            var schema = new Schema(new[] { new Field("Value", global::Apache.Arrow.Types.Int32Type.Default, false) }, null);
            var source = Observable.Create<RecordBatch>(observer =>
            {
                for (int i = 0; i < 50; i++)
                {
                    using var batch = new RecordBatch(schema,
                        new IArrowArray[] { new Int32Array.Builder().Append(i).Build() }, 1);
                    observer.OnNext(batch);
                }
                observer.OnCompleted();
                return Disposable.Empty;
            });
            var writer = (ArrowWriter)CreateWriter("Arrow", true);
            Assert.AreEqual(50, writer.Process(source).Count().Timeout(TimeSpan.FromSeconds(10)).Wait());
            CollectionAssert.AreEqual(Enumerable.Range(0, 50).ToArray(), Read("Arrow"));
        }

        [TestMethod]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", true)]
        public void ConversionErrorsDuringTimedFlushReachAnIdleWorkflow(string format, bool buffered)
        {
            using var source = new Subject<decimal>();
            using var ended = new ManualResetEventSlim();
            Exception? failure = null;
            using var subscription = Write(CreateWriter(format, buffered, flushMilliseconds: 20), source)
                .Subscribe(_ => { }, ex => { failure = ex; ended.Set(); });
            source.OnNext(decimal.MaxValue); // Cannot fit precision 28, scale 12.
            Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
            Assert.IsNotNull(failure);
            // The error is delivered after the writer releases its file handle.
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
        }

        [TestMethod]
        [DataRow("Arrow", "QueueCapacity")]
        [DataRow("Parquet", "QueueCapacity")]
        [DataRow("Arrow", "FlushInterval")]
        [DataRow("Parquet", "FlushInterval")]


        public void InvalidSchedulingOptionsDoNotTruncateExistingFiles(string format, string property)
        {
            File.WriteAllText(path, "original");
            var writer = CreateWriter(format, true);
            writer.GetType().GetProperty("Overwrite")!.SetValue(writer, true);
            var info = writer.GetType().GetProperty(property)!;
            object invalid = property == "FlushInterval" ? (object)TimeSpan.FromSeconds(-2)
                : 0;
            info.SetValue(writer, invalid);
            Assert.ThrowsExactly<InvalidOperationException>(() => Write(writer, Observable.Return(1)).Wait());
            Assert.AreEqual("original", File.ReadAllText(path));
        }

        [TestMethod]
        public void BothOperatorsExposeTheSameSchedulingSettingsAndDefaults()
        {
            var arrow = new ArrowWriter();
            var parquet = new Parquet.ParquetWriter();
            var properties = typeof(WriterOptions).GetProperties().OrderBy(p => p.Name).ToArray();
            var other = typeof(Parquet.WriterOptions).GetProperties().OrderBy(p => p.Name).ToArray();
            CollectionAssert.AreEqual(properties.Select(p => p.Name).ToArray(), other.Select(p => p.Name).ToArray());
            for (int i = 0; i < properties.Length; i++)
            {
                Assert.AreEqual(properties[i].PropertyType.Name, other[i].PropertyType.Name, properties[i].Name);
                Assert.AreEqual(properties[i].GetValue(arrow)?.ToString(), other[i].GetValue(parquet)?.ToString(), properties[i].Name);
            }
        }

        [TestMethod]
        public void SchedulingSettingsRoundTripThroughTheBonsaiWorkflowSerializer()
        {
            var graph = new ExpressionBuilderGraph();
            foreach (var format in new[] { "Arrow", "Parquet" })
                graph.Add(new CombinatorBuilder { Combinator = CreateWriter(format, true, 13, 123) });
            var workflow = new WorkflowBuilder(graph);
            using var text = new StringWriter();
            WorkflowBuilder.Serializer.Serialize(text, workflow);
            Assert.IsTrue(text.ToString().Contains("PT0.123S"));
            using var xml = XmlReader.Create(new StringReader(text.ToString()));
            var restored = (WorkflowBuilder)WorkflowBuilder.Serializer.Deserialize(xml)!;
            foreach (var node in restored.Workflow)
            {
                var writer = ((CombinatorBuilder)node.Value).Combinator;
                Assert.AreEqual(TimeSpan.FromMilliseconds(123), writer.GetType().GetProperty("FlushInterval")!.GetValue(writer));
                Assert.AreEqual(true, writer.GetType().GetProperty("Buffered")!.GetValue(writer));
                Assert.AreEqual(13, writer.GetType().GetProperty("BatchSize")!.GetValue(writer));
                Assert.AreEqual(2048, writer.GetType().GetProperty("QueueCapacity")!.GetValue(writer));

            }
        }
    }
}
