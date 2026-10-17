using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using global::Apache.Arrow;
using global::Apache.Arrow.Compression;
using global::Apache.Arrow.Ipc;
using global::Apache.Arrow.Types;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class ArrowWriterTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".arrow");

        [TestCleanup]
        public void Cleanup() { if (File.Exists(path)) File.Delete(path); }

        void Write<T>(IEnumerable<T> rows, int batchSize = 2, bool compression = false, string? selector = null)
        {
            var writer = new ArrowWriter { Buffered = false, FileName = path, BatchSize = batchSize, EnableCompression = compression, Selector = selector };
            CollectionAssert.AreEqual(rows.ToArray(), writer.Process(rows.ToObservable()).ToArray().Wait());
        }

        static Schema IntSchema(string name = "Value") => new Schema(new[] { new Field(name, Int32Type.Default, false) }, null);
        static RecordBatch IntBatch(IList<int> rows, Schema schema) => new RecordBatch(schema,
            new IArrowArray[] { new Int32Array.Builder().AppendRange(rows).Build() }, rows.Count);

        [TestMethod]
        public void BatchBoundariesAndFooterAreWrittenBeforeCompletion()
        {
            Write(Enumerable.Range(0, 5));
            using var reader = new ArrowFileReader(File.OpenRead(path));
            Assert.AreEqual(3, reader.RecordBatchCountAsync().GetAwaiter().GetResult());
            var actual = new List<int>();
            for (int i = 0; i < 3; i++)
            {
                using var batch = reader.ReadNextRecordBatch();
                actual.AddRange(((Int32Array)batch.Column(0)).Values.ToArray());
            }
            CollectionAssert.AreEqual(Enumerable.Range(0, 5).ToArray(), actual);
        }

        [TestMethod]
        public void EmptySourceCreatesValidSchemaOnlyFile()
        {
            Write(Enumerable.Empty<int>());
            using var reader = new ArrowFileReader(File.OpenRead(path));
            Assert.AreEqual(0, reader.RecordBatchCountAsync().GetAwaiter().GetResult());
            Assert.AreEqual("Value", reader.GetSchema().GetAwaiter().GetResult().GetFieldByIndex(0).Name);
        }

        [TestMethod]
        public void NullableScalarRoundTrip()
        {
            Write(new int?[] { 1, null, 3 });
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = ArrowColumnType.Int32, IsNullable = true });
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, reader.Generate<int?>().ToArray().Wait());
        }

        [TestMethod]
        public void EveryScalarTypeRoundTrips()
        {
            RoundTrip(true, ArrowColumnType.Bool);
            RoundTrip((sbyte)-9, ArrowColumnType.SByte);
            RoundTrip(byte.MaxValue, ArrowColumnType.Byte);
            RoundTrip(short.MinValue, ArrowColumnType.Int16);
            RoundTrip(ushort.MaxValue, ArrowColumnType.UInt16);
            RoundTrip(int.MinValue, ArrowColumnType.Int32);
            RoundTrip(uint.MaxValue, ArrowColumnType.UInt32);
            RoundTrip(long.MinValue, ArrowColumnType.Int64);
            RoundTrip(ulong.MaxValue, ArrowColumnType.UInt64);
            RoundTrip(1.25f, ArrowColumnType.Float);
            RoundTrip(-2.5, ArrowColumnType.Double);
            RoundTrip(123.456m, ArrowColumnType.Decimal);
            RoundTrip("Bonsai 🌳", ArrowColumnType.String);
            RoundTrip(new DateTime(2020, 1, 1, 12, 34, 56, DateTimeKind.Unspecified), ArrowColumnType.DateTime);
            RoundTrip(TimeSpan.FromHours(-27), ArrowColumnType.TimeSpan);
            RoundTrip(Guid.NewGuid(), ArrowColumnType.Guid);
        }

        void RoundTrip<T>(T value, ArrowColumnType type)
        {
            if (File.Exists(path)) File.Delete(path);
            Write(new[] { value });
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = type });
            Assert.AreEqual(value, reader.Generate<T>().SingleAsync().Wait());
        }

        [TestMethod]
        public void NullableScalarConvertersPreserveNullsAcrossBatches()
        {
            NullableRoundTrip(true, ArrowColumnType.Bool);
            NullableRoundTrip(sbyte.MinValue, ArrowColumnType.SByte);
            NullableRoundTrip(byte.MaxValue, ArrowColumnType.Byte);
            NullableRoundTrip(short.MinValue, ArrowColumnType.Int16);
            NullableRoundTrip(ushort.MaxValue, ArrowColumnType.UInt16);
            NullableRoundTrip(int.MinValue, ArrowColumnType.Int32);
            NullableRoundTrip(uint.MaxValue, ArrowColumnType.UInt32);
            NullableRoundTrip(long.MinValue, ArrowColumnType.Int64);
            NullableRoundTrip(ulong.MaxValue, ArrowColumnType.UInt64);
            NullableRoundTrip(1.25f, ArrowColumnType.Float);
            NullableRoundTrip(-2.5, ArrowColumnType.Double);
            NullableRoundTrip(123.456m, ArrowColumnType.Decimal);
            NullableRoundTrip(new DateTime(2020, 1, 1, 12, 34, 56, DateTimeKind.Local), ArrowColumnType.DateTime);
            NullableRoundTrip(TimeSpan.FromHours(-27), ArrowColumnType.TimeSpan);
            NullableRoundTrip(Guid.NewGuid(), ArrowColumnType.Guid);
        }

        void NullableRoundTrip<T>(T value, ArrowColumnType type) where T : struct
        {
            if (File.Exists(path)) File.Delete(path);
            var expected = new T?[] { null, value, null };
            Write(expected);
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = type, IsNullable = true });
            CollectionAssert.AreEqual(expected, reader.Generate<T?>().ToArray().Wait());
        }

        enum WideState : ulong { Maximum = ulong.MaxValue }

        readonly struct TypedRow
        {
            public TypedRow(int index, WideState? state) { Index = index; State = state; }
            public int Index { get; }
            public WideState? State { get; }
        }

        [TestMethod]
        public void StructRowsAndUnsignedNullableEnumsKeepTheirValues()
        {
            Write(new[] { new TypedRow(1, WideState.Maximum), new TypedRow(2, null), new TypedRow(3, WideState.Maximum) });
            using var file = new ArrowFileReader(File.OpenRead(path));
            using var first = file.ReadNextRecordBatch();
            Assert.AreEqual(1, ((Int32Array)first.Column(0)).GetValue(0));
            Assert.AreEqual(2, ((Int32Array)first.Column(0)).GetValue(1));
            Assert.AreEqual(ulong.MaxValue, ((UInt64Array)first.Column(1)).GetValue(0));
            Assert.IsTrue(first.Column(1).IsNull(1));
            using var last = file.ReadNextRecordBatch();
            Assert.AreEqual(3, ((Int32Array)last.Column(0)).GetValue(0));
            Assert.AreEqual(ulong.MaxValue, ((UInt64Array)last.Column(1)).GetValue(0));
        }

        [TestMethod]
        public void ReferenceColumnsPreserveNullAndEmptyValues()
        {
            Write(new[] { new { Text = (string?)null, Data = (byte[]?)null },
                new { Text = (string?)"", Data = (byte[]?)System.Array.Empty<byte>() },
                new { Text = (string?)"Bonsai 🌳", Data = (byte[]?)new byte[] { 0, 255 } } });
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Text", Type = ArrowColumnType.String });
            reader.Columns.Add(new ColumnDefinition { Name = "Data", Type = ArrowColumnType.Bytes });
            var actual = reader.Generate<Tuple<string?, byte[]?>>().ToArray().Wait();
            Assert.IsNull(actual[0].Item1);
            Assert.IsNull(actual[0].Item2);
            Assert.AreEqual("", actual[1].Item1);
            CollectionAssert.AreEqual(System.Array.Empty<byte>(), actual[1].Item2);
            Assert.AreEqual("Bonsai 🌳", actual[2].Item1);
            CollectionAssert.AreEqual(new byte[] { 0, 255 }, actual[2].Item2);
        }

        [TestMethod]
        public void ListsPreserveNullAndEmptyAndBinaryRemainsBinary()
        {
            Write(new[] { new int[] { 1, 2 }, null, new int[0], new int[] { 3 } });
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = ArrowColumnType.Int32, IsArray = true });
            var rows = reader.Generate<int[]?>().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1, 2 }, rows[0]);
            Assert.IsNull(rows[1]);
            Assert.HasCount(0, rows[2]);
            File.Delete(path);
            Write(new[] { new byte[] { 0, 1, 255 } });
            using var file = new ArrowFileReader(File.OpenRead(path));
            using var batch = file.ReadNextRecordBatch();
            Assert.IsInstanceOfType<BinaryArray>(batch.Column(0));
        }

        [TestMethod]
        public void EnumerableAndNullableEnumsUseSharedInference()
        {
            Write(new[] { new { Values = (IList<int>)new List<int> { 1, 2 }, State = (DayOfWeek?)DayOfWeek.Friday } });
            using var file = new ArrowFileReader(File.OpenRead(path));
            using var batch = file.ReadNextRecordBatch();
            Assert.IsInstanceOfType<ListArray>(batch.Column(0));
            Assert.AreEqual(5, ((Int32Array)batch.Column(1)).GetValue(0));
        }

        [TestMethod]
        public void NestedSelectorsAndColumnsMatchParquet()
        {
            var row = new { Inner = new { Value = 42 }, Ignored = new object() };
            Write(new[] { row }, selector: "Inner.Value");
            using var file = new ArrowFileReader(File.OpenRead(path));
            using var batch = file.ReadNextRecordBatch();
            Assert.AreEqual("Inner.Value", batch.Schema.GetFieldByIndex(0).Name);
            Assert.AreEqual(42, ((Int32Array)batch.Column(0)).GetValue(0));
            var parquetPath = path + ".parquet";
            try
            {
                new Bonsai.Apache.Parquet.ParquetWriter { FileName = parquetPath, Selector = "Inner.Value" }.Process(Observable.Return(row)).Wait();
                using var parquet = new ParquetSharp.ParquetFileReader(parquetPath);
                Assert.AreEqual(batch.Schema.GetFieldByIndex(0).Name, parquet.FileMetaData.Schema.Column(0).Name);
            }
            finally { if (File.Exists(parquetPath)) File.Delete(parquetPath); }
        }

        [TestMethod]
        public void CompressionCanBeReadByArrowAndTypedReader()
        {
            Write(Enumerable.Range(0, 1000), batchSize: 128, compression: true);
            var reader = new ArrowReader { FileName = path };
            reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = ArrowColumnType.Int32 });
            CollectionAssert.AreEqual(Enumerable.Range(0, 1000).ToArray(), reader.Generate<int>().ToArray().Wait());
        }

        [TestMethod]
        public void DirectBatchesRetainOwnershipAndAcceptEquivalentSchemas()
        {
            using var batch1 = IntBatch(new[] { 1, 2 }, IntSchema());
            using var batch2 = IntBatch(new[] { 3 }, IntSchema());
            var output = new ArrowWriter { FileName = path }.Process(new[] { batch1, batch2 }.ToObservable()).ToArray().Wait();
            Assert.AreSame(batch1, output[0]);
            Assert.AreEqual(1, ((Int32Array)batch1.Column(0)).GetValue(0));
            using var reader = new ArrowFileReader(File.OpenRead(path));
            Assert.AreEqual(2, reader.RecordBatchCountAsync().GetAwaiter().GetResult());
        }

        [TestMethod]
        public void DirectBatchesRejectSchemaChanges()
        {
            using var batch1 = IntBatch(new[] { 1 }, IntSchema());
            using var batch2 = IntBatch(new[] { 2 }, IntSchema("Other"));
            Assert.Throws<InvalidOperationException>(() => new ArrowWriter { FileName = path }
                .Process(new[] { batch1, batch2 }.ToObservable()).Wait());
        }

        [TestMethod]
        public void CancellationAndSourceErrorsFlushPendingRows()
        {
            using var source = new Subject<int>();
            var subscription = new ArrowWriter { FileName = path, Buffered = false }.Process(source).Subscribe();
            source.OnNext(42);
            subscription.Dispose();
            using (var reader = new ArrowFileReader(File.OpenRead(path)))
            using (var batch = reader.ReadNextRecordBatch()) Assert.AreEqual(42, ((Int32Array)batch.Column(0)).GetValue(0));
            File.Delete(path);
            var error = new InvalidOperationException("source failure");
            Exception? observed = null;
            using var subscription2 = new ArrowWriter { FileName = path, Buffered = false }.Process(source).Subscribe(_ => { }, ex => observed = ex);
            source.OnNext(43);
            source.OnError(error);
            Assert.AreSame(error, observed);
            using var file = new ArrowFileReader(File.OpenRead(path));
            using var last = file.ReadNextRecordBatch();
            Assert.AreEqual(43, ((Int32Array)last.Column(0)).GetValue(0));
        }

        [TestMethod]
        public void InvalidSchemaAndBatchSizeDoNotTruncateExistingFiles()
        {
            File.WriteAllText(path, "original");
            var writer = new ArrowWriter { FileName = path, Overwrite = true, Selector = "Missing" };
            Assert.Throws<InvalidOperationException>(() => writer.Process(Observable.Return(1)).Wait());
            writer.Selector = null;
            writer.BatchSize = 0;
            Assert.Throws<InvalidOperationException>(() => writer.Process(Observable.Return(1)).Wait());
            Assert.AreEqual("original", File.ReadAllText(path));
            writer.BatchSize = 1;
            writer.Overwrite = false;
            Assert.Throws<IOException>(() => writer.Process(Observable.Return(1)).Wait());
        }

        [TestMethod]
        public void CustomBatchConverterExpandsFramesOnExplicitFlush()
        {
            var schema = IntSchema();
            var sink = new ArrowBatchWriter<int[]>(path, schema, 10,
                (frames, s) => IntBatch(frames.SelectMany(f => f).ToArray(), s));
            sink.Write(new[] { 1, 2, 3 });
            sink.Flush();
            sink.Dispose();
            sink.Dispose();
            using var reader = new ArrowFileReader(File.OpenRead(path));
            using var result = reader.ReadNextRecordBatch();
            Assert.AreEqual(3, result.Length);
        }

        [TestMethod]
        public void ConverterFailureIsReportedDirectlyAndNotRetriedOnDispose()
        {
            int calls = 0;
            var sink = new ArrowBatchWriter<int>(path, IntSchema(), 10, (items, s) =>
            { calls++; throw new InvalidOperationException("conversion failed"); });
            sink.Write(1);
            Assert.Throws<InvalidOperationException>(sink.Flush);
            sink.Dispose();
            sink.Dispose();
            Assert.AreEqual(1, calls);
        }

        [TestMethod]
        public void FullBatchConversionFailureIsReportedThroughObservable()
        {
            using var source = new Subject<decimal>();
            Exception? observed = null;
            using var subscription = new ArrowWriter { FileName = path, BatchSize = 1, Buffered = false }
                .Process(source).Subscribe(_ => Assert.Fail("Invalid row should not pass through."), ex => observed = ex);
            source.OnNext(decimal.MaxValue);
            Assert.IsNotNull(observed);
            using var reader = new ArrowFileReader(File.OpenRead(path));
            Assert.AreEqual(0, reader.RecordBatchCountAsync().GetAwaiter().GetResult());
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void DirectBatchesPreserveRunEndEncodingAndMetadata(bool buffered)
        {
            var values = new Int16Array.Builder().AppendRange(new short[] { 11, 22 }).Build();
            var ends = new Int32Array.Builder().AppendRange(new[] { 3, 6 }).Build();
            var encoded = new RunEndEncodedArray(ends, values);
            var schema = new Schema(new[] { new Field("Channel0", encoded.Data.DataType, false) },
                new Dictionary<string, string> { { "device", "test-device" } });
            using var batch = new RecordBatch(schema, new IArrowArray[] { encoded }, 6);
            new ArrowWriter { FileName = path, EnableCompression = true, Buffered = buffered }.Process(Observable.Return(batch)).Wait();
            using var reader = new ArrowFileReader(File.OpenRead(path), new CompressionCodecFactory());
            using var result = reader.ReadNextRecordBatch();
            Assert.AreEqual("test-device", result.Schema.Metadata["device"]);
            Assert.IsInstanceOfType<RunEndEncodedArray>(result.Column(0));
            Assert.AreEqual(6, result.Length);
        }

        [TestMethod]
        public void ReaderBuildSupportsEightColumnsWithSharedTupleFactory()
        {
            var reader = new ArrowReader();
            for (int i = 0; i < 8; i++) reader.Columns.Add(new ColumnDefinition { Name = "C" + i, Type = ArrowColumnType.Int32 });
            var expression = reader.Build(new System.Linq.Expressions.Expression[0]);
            Assert.AreEqual(typeof(IObservable<Tuple<int, int, int, int, int, int, int, Tuple<int>>>), expression.Type);
        }
    }
}
