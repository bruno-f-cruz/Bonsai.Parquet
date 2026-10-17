using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using ParquetSharp;

namespace Bonsai.Apache.Parquet.Tests
{
    [TestClass]
    public class ColumnBufferTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".parquet");

        [TestCleanup]
        public void Cleanup() { if (File.Exists(path)) File.Delete(path); }

        void Write<T>(IEnumerable<T> rows, int size = 2, string? selector = null)
            => new ParquetWriter { Buffered = false, FileName = path, Overwrite = true, RowGroupSize = size, Selector = selector }
                .Process(rows.ToObservable()).DefaultIfEmpty().Wait();

        T[] Read<T>(int index = 0)
        {
            using var reader = new ParquetFileReader(path);
            var rows = new List<T>();
            for (int i = 0; i < reader.FileMetaData.NumRowGroups; i++)
            {
                using var group = reader.RowGroup(i);
                using var column = group.Column(index).LogicalReader<T>();
                rows.AddRange(column.ReadAll((int)group.MetaData.NumRows));
            }
            return rows.ToArray();
        }

        [TestMethod]
        [DataRow(Compression.Uncompressed)]
        [DataRow(Compression.Snappy)]
        [DataRow(Compression.Gzip)]
        [DataRow(Compression.Brotli)]
        [DataRow(Compression.Zstd)]
        public void SelectedCompressionCodecIsWrittenToTheFile(Compression compression)
        {
            var expected = Enumerable.Range(0, 20).ToArray();
            var writer = new ParquetWriter
            {
                FileName = path, CompressionMethod = compression, RowGroupSize = 5,
                Buffered = true, QueueCapacity = 32
            };
            writer.Process(expected.ToObservable()).Timeout(TimeSpan.FromSeconds(5)).Wait();
            CollectionAssert.AreEqual(expected, Read<int>());
            using var file = new ParquetFileReader(path);
            using var group = file.RowGroup(0);
            var metadata = group.MetaData;
            using var column = metadata.GetColumnChunkMetaData(0);
            Assert.AreEqual(compression, column.Compression);
        }

        [TestMethod]
        public void BuffersGrowAndWriteOnlyTheValidRowsOfTheLastGroup()
        {
            var expected = Enumerable.Range(0, 4101).ToArray();
            Write(expected, size: 2049);
            CollectionAssert.AreEqual(expected, Read<int>());
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(3, reader.FileMetaData.NumRowGroups);
            using var last = reader.RowGroup(2);
            Assert.AreEqual(3L, last.MetaData.NumRows);
        }

        enum WideState : ulong { Maximum = ulong.MaxValue }

        readonly struct Sample
        {
            public Sample(int index, WideState? state) { Index = index; State = state; }
            public int Index { get; }
            public WideState? State { get; }
        }

        [TestMethod]
        public void StructRowsPreserveUnsignedNullableEnums()
        {
            Write(new[] { new Sample(1, WideState.Maximum), new Sample(2, null), new Sample(3, WideState.Maximum) });
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, Read<int>());
            CollectionAssert.AreEqual(new ulong?[] { ulong.MaxValue, null, ulong.MaxValue }, Read<ulong?>(1));
        }

        [TestMethod]
        public void NullableScalarColumnsPreserveNullsAcrossGroups()
        {
            NullableRoundTrip(true);
            NullableRoundTrip(sbyte.MinValue);
            NullableRoundTrip(byte.MaxValue);
            NullableRoundTrip(short.MinValue);
            NullableRoundTrip(ushort.MaxValue);
            NullableRoundTrip(int.MinValue);
            NullableRoundTrip(uint.MaxValue);
            NullableRoundTrip(long.MinValue);
            NullableRoundTrip(ulong.MaxValue);
            NullableRoundTrip(1.25f);
            NullableRoundTrip(-2.5);
            NullableRoundTrip(123.456m);
            NullableRoundTrip(new DateTime(2020, 1, 1, 12, 34, 56));
            NullableRoundTrip(TimeSpan.FromHours(-27));
            NullableRoundTrip(Guid.NewGuid());
        }

        void NullableRoundTrip<T>(T value) where T : struct
        {
            var expected = new T?[] { null, value, null };
            Write(expected);
            CollectionAssert.AreEqual(expected, Read<T?>());
        }

        [TestMethod]
        public void ReferenceColumnsPreserveNullAndEmptyValuesAcrossGroups()
        {
            var rows = new[]
            {
                new { Text = (string?)null, Binary = (byte[]?)null, Values = (IList<int>?)null },
                new { Text = (string?)"", Binary = (byte[]?)Array.Empty<byte>(), Values = (IList<int>?)new List<int>() },
                new { Text = (string?)"third", Binary = (byte[]?)new byte[] { 0, 255 }, Values = (IList<int>?)new List<int> { 3, 4 } },
                new { Text = (string?)null, Binary = (byte[]?)null, Values = (IList<int>?)null },
                new { Text = (string?)"last", Binary = (byte[]?)new byte[] { 5 }, Values = (IList<int>?)new List<int> { 5 } }
            };
            Write(rows);
            CollectionAssert.AreEqual(rows.Select(r => r.Text).ToArray(), Read<string?>());
            var binary = Read<byte[]?>(1);
            var lists = Read<int[]?>(2);
            for (int i = 0; i < rows.Length; i++)
            {
                CollectionAssert.AreEqual(rows[i].Binary, binary[i]);
                CollectionAssert.AreEqual(rows[i].Values?.ToArray(), lists[i]);
            }
        }

        sealed class CollectionRow
        {
            public int Calls;
            public IEnumerable<int> Values { get { Calls++; return Enumerable.Range(1, 3); } }
        }

        [TestMethod]
        public void CollectionGettersAreEvaluatedOncePerRow()
        {
            var rows = new[] { new CollectionRow(), new CollectionRow(), new CollectionRow() };
            Write(rows, selector: "Values");
            Assert.IsTrue(rows.All(row => row.Calls == 1));
            foreach (var value in Read<int[]>()) CollectionAssert.AreEqual(new[] { 1, 2, 3 }, value);
        }

        sealed class FallibleRow
        {
            public string Label { get; set; } = "";
            public bool Fail;
            public int Value => Fail ? throw new InvalidOperationException("bad row") : Label.Length;
        }

        [TestMethod]
        public void GetterFailureDiscardsThePartialRowAndKeepsEarlierRows()
        {
            using (var sink = new ParquetBatchWriter<FallibleRow>(path, 2, selector: "Label,Value"))
            {
                sink.Write(new FallibleRow { Label = "first" });
                Assert.ThrowsExactly<InvalidOperationException>(() => sink.Write(new FallibleRow { Label = "bad", Fail = true }));
                sink.Write(new FallibleRow { Label = "last" });
            }
            CollectionAssert.AreEqual(new[] { "first", "last" }, Read<string>());
            CollectionAssert.AreEqual(new[] { 5, 4 }, Read<int>(1));
        }

        [TestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        public void InvalidRowGroupSizeDoesNotTruncateExistingFile(int size)
        {
            File.WriteAllText(path, "original");
            Assert.ThrowsExactly<InvalidOperationException>(() => Write(new[] { 1 }, size));
            Assert.AreEqual("original", File.ReadAllText(path));
        }
    }
}
