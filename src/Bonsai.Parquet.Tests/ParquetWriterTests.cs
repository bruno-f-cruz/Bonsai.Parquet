using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using Bonsai.IO;
using Bonsai.Parquet;
using ParquetSharp;

namespace Bonsai.Parquet.Tests
{
    [TestClass]
    public sealed class ParquetWriterTests
    {
        // ─── helpers ────────────────────────────────────────────────────────────

        static string TempFile() => Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".parquet");

        static T[] ReadColumn<T>(string path, int columnIndex = 0)
        {
            using var reader = new ParquetFileReader(path);
            var results = new List<T>();
            for (int rg = 0; rg < reader.FileMetaData.NumRowGroups; rg++)
            {
                using var group = reader.RowGroup(rg);
                using var colReader = group.Column(columnIndex).LogicalReader<T>();
                results.AddRange(colReader.ReadAll((int)group.MetaData.NumRows));
            }
            return results.ToArray();
        }

        static void Write<T>(string path, IEnumerable<T> rows, int rowGroupSize = 100_000, bool overwrite = true)
        {
            var writer = new ParquetWriter { FileName = path, Overwrite = overwrite, RowGroupSize = rowGroupSize };
            writer.Process(rows.ToObservable()).DefaultIfEmpty().Wait();
        }

        // ─── round-trip type mapping tests ──────────────────────────────────────

        [TestMethod]
        public void RoundTrip_Bool()
        {
            var path = TempFile();
            Write(path, new[] { true, false, true });
            CollectionAssert.AreEqual(new[] { true, false, true }, ReadColumn<bool>(path));
        }

        [TestMethod]
        public void RoundTrip_Int()
        {
            var path = TempFile();
            Write(path, new[] { 1, -2, int.MaxValue });
            CollectionAssert.AreEqual(new[] { 1, -2, int.MaxValue }, ReadColumn<int>(path));
        }

        [TestMethod]
        public void RoundTrip_UInt()
        {
            var path = TempFile();
            Write(path, new[] { 0u, 1u, uint.MaxValue });
            CollectionAssert.AreEqual(new[] { 0u, 1u, uint.MaxValue }, ReadColumn<uint>(path));
        }

        [TestMethod]
        public void RoundTrip_Long()
        {
            var path = TempFile();
            Write(path, new[] { 0L, long.MinValue, long.MaxValue });
            CollectionAssert.AreEqual(new[] { 0L, long.MinValue, long.MaxValue }, ReadColumn<long>(path));
        }

        [TestMethod]
        public void RoundTrip_ULong()
        {
            var path = TempFile();
            Write(path, new[] { 0uL, 1uL, ulong.MaxValue });
            CollectionAssert.AreEqual(new[] { 0uL, 1uL, ulong.MaxValue }, ReadColumn<ulong>(path));
        }

        [TestMethod]
        public void RoundTrip_Short()
        {
            var path = TempFile();
            Write(path, new short[] { -1, 0, short.MaxValue });
            CollectionAssert.AreEqual(new short[] { -1, 0, short.MaxValue }, ReadColumn<short>(path));
        }

        [TestMethod]
        public void RoundTrip_UShort()
        {
            var path = TempFile();
            Write(path, new ushort[] { 0, 1, ushort.MaxValue });
            CollectionAssert.AreEqual(new ushort[] { 0, 1, ushort.MaxValue }, ReadColumn<ushort>(path));
        }

        [TestMethod]
        public void RoundTrip_Byte()
        {
            var path = TempFile();
            Write(path, new byte[] { 0, 127, 255 });
            CollectionAssert.AreEqual(new byte[] { 0, 127, 255 }, ReadColumn<byte>(path));
        }

        [TestMethod]
        public void RoundTrip_SByte()
        {
            var path = TempFile();
            Write(path, new sbyte[] { -128, 0, 127 });
            CollectionAssert.AreEqual(new sbyte[] { -128, 0, 127 }, ReadColumn<sbyte>(path));
        }

        [TestMethod]
        public void RoundTrip_Float()
        {
            var path = TempFile();
            Write(path, new[] { 1.5f, -2.25f, float.NaN });
            var actual = ReadColumn<float>(path);
            Assert.AreEqual(3, actual.Length);
            Assert.AreEqual(1.5f, actual[0]);
            Assert.AreEqual(-2.25f, actual[1]);
            Assert.IsTrue(float.IsNaN(actual[2]));
        }

        [TestMethod]
        public void RoundTrip_Double()
        {
            var path = TempFile();
            Write(path, new[] { 1.0, -2.5, double.PositiveInfinity });
            CollectionAssert.AreEqual(new[] { 1.0, -2.5, double.PositiveInfinity }, ReadColumn<double>(path));
        }

        [TestMethod]
        public void RoundTrip_Decimal()
        {
            var path = TempFile();
            Write(path, new[] { 1.23m, -4.56m, 0m });
            CollectionAssert.AreEqual(new[] { 1.23m, -4.56m, 0m }, ReadColumn<decimal>(path));
        }

        [TestMethod]
        public void RoundTrip_String()
        {
            var path = TempFile();
            Write(path, new[] { "hello", "world", "" });
            CollectionAssert.AreEqual(new[] { "hello", "world", "" }, ReadColumn<string?>(path));
        }

        [TestMethod]
        public void RoundTrip_ByteArray()
        {
            var path = TempFile();
            var data = new[] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5 } };
            Write(path, data);
            var actual = ReadColumn<byte[]?>(path);
            Assert.AreEqual(2, actual.Length);
            CollectionAssert.AreEqual(data[0], actual[0]);
            CollectionAssert.AreEqual(data[1], actual[1]);
        }

        [TestMethod]
        public void RoundTrip_DateTime()
        {
            var path = TempFile();
            // Use microsecond-precision values since parquet stores MICROS
            var dt1 = new DateTime(2024, 1, 15, 10, 30, 0, 123, DateTimeKind.Unspecified);
            var dt2 = new DateTime(2000, 6, 1, 0, 0, 0, 0, DateTimeKind.Unspecified);
            Write(path, new[] { dt1, dt2 });
            var actual = ReadColumn<DateTime>(path);
            Assert.AreEqual(dt1, actual[0]);
            Assert.AreEqual(dt2, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_TimeSpan()
        {
            var path = TempFile();
            var ts1 = TimeSpan.FromMilliseconds(1234);
            var ts2 = TimeSpan.Zero;
            Write(path, new[] { ts1, ts2 });
            var actual = ReadColumn<TimeSpan>(path);
            Assert.AreEqual(ts1, actual[0]);
            Assert.AreEqual(ts2, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_Guid()
        {
            var path = TempFile();
            var g1 = Guid.NewGuid();
            var g2 = Guid.Empty;
            Write(path, new[] { g1, g2 });
            var actual = ReadColumn<Guid>(path);
            Assert.AreEqual(g1, actual[0]);
            Assert.AreEqual(g2, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_NullableInt_NonNull()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, 2, 3 });
            CollectionAssert.AreEqual(new int?[] { 1, 2, 3 }, ReadColumn<int?>(path));
        }

        [TestMethod]
        public void RoundTrip_NullableInt_WithNull()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, null, 3 });
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, ReadColumn<int?>(path));
        }

        [TestMethod]
        public void RoundTrip_IntArray()
        {
            var path = TempFile();
            Write(path, new[] { new[] { 1, 2, 3 }, new[] { 4, 5 } });
            var actual = ReadColumn<int[]?>(path);
            Assert.AreEqual(2, actual.Length);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, actual[0]);
            CollectionAssert.AreEqual(new[] { 4, 5 }, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_StringArray()
        {
            var path = TempFile();
            Write(path, new[] { new[] { "a", "b" }, new[] { "c" } });
            var actual = ReadColumn<string[]?>(path);
            Assert.AreEqual(2, actual.Length);
            CollectionAssert.AreEqual(new[] { "a", "b" }, actual[0]);
            CollectionAssert.AreEqual(new[] { "c" }, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_Enum()
        {
            var path = TempFile();
            Write(path, new[] { TestEnum.A, TestEnum.B, TestEnum.C });
            // Enum stored as underlying int
            var actual = ReadColumn<int>(path);
            CollectionAssert.AreEqual(new[] { (int)TestEnum.A, (int)TestEnum.B, (int)TestEnum.C }, actual);
        }

        // ─── schema inference tests ─────────────────────────────────────────────

        [TestMethod]
        public void Schema_AnonymousType_ProducesExpectedColumns()
        {
            var path = TempFile();
            var rows = Enumerable.Range(0, 3)
                .Select(i => new { X = i, Y = (double)i, Label = i.ToString() })
                .ToObservable();
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            using var reader = new ParquetFileReader(path);
            var schema = reader.FileMetaData.Schema;
            Assert.AreEqual(3, schema.NumColumns);
            Assert.AreEqual("X", schema.Column(0).Name);
            Assert.AreEqual("Y", schema.Column(1).Name);
            Assert.AreEqual("Label", schema.Column(2).Name);
        }

        [TestMethod]
        public void Schema_SinglePrimitive_ProducesSingleValueColumn()
        {
            var path = TempFile();
            Write(path, new[] { 42 });
            using var reader = new ParquetFileReader(path);
            var schema = reader.FileMetaData.Schema;
            Assert.AreEqual(1, schema.NumColumns);
            Assert.AreEqual("Value", schema.Column(0).Name);
        }

        // ─── row-group boundary tests ───────────────────────────────────────────

        [TestMethod]
        public void RowGroup_ExactlyRowGroupSize_ProducesOneRowGroup()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 5), rowGroupSize: 5);
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(1, reader.FileMetaData.NumRowGroups);
            using (var rg = reader.RowGroup(0)) Assert.AreEqual(5L, rg.MetaData.NumRows);
        }

        [TestMethod]
        public void RowGroup_LessThanRowGroupSize_ProducesOneRowGroup()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 4), rowGroupSize: 5);
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(1, reader.FileMetaData.NumRowGroups);
            using (var rg = reader.RowGroup(0)) Assert.AreEqual(4L, rg.MetaData.NumRows);
        }

        [TestMethod]
        public void RowGroup_MoreThanRowGroupSize_ProducesTwoRowGroups()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 6), rowGroupSize: 5);
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(2, reader.FileMetaData.NumRowGroups);
            using (var rg0 = reader.RowGroup(0)) Assert.AreEqual(5L, rg0.MetaData.NumRows);
            using (var rg1 = reader.RowGroup(1)) Assert.AreEqual(1L, rg1.MetaData.NumRows);
        }

        [TestMethod]
        public void RowGroup_ZeroRows_ProducesValidEmptyFile()
        {
            var path = TempFile();
            Write(path, Enumerable.Empty<int>());
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(0, reader.FileMetaData.NumRowGroups);
        }

        // ─── lifecycle tests ─────────────────────────────────────────────────────

        [TestMethod]
        public void Lifecycle_NonEmptyBuffer_FlushesOnDispose()
        {
            var path = TempFile();
            // rowGroupSize > number of rows — tail group must flush on dispose
            Write(path, new[] { 1, 2, 3 }, rowGroupSize: 1000);
            var actual = ReadColumn<int>(path);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, actual);
        }

        [TestMethod]
        public void Lifecycle_EmptyStream_ProducesValidFile()
        {
            var path = TempFile();
            Write(path, Enumerable.Empty<int>());
            Assert.IsTrue(File.Exists(path));
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(0L, reader.FileMetaData.NumRows);
        }

        [TestMethod]
        public void Lifecycle_DoubleDispose_IsSafe()
        {
            var path = TempFile();
            var sink = new ParquetSinkTestHarness<int>(path);
            sink.Sink.AppendRow(1);
            sink.Sink.Dispose();
            // Second dispose must not throw
            sink.Sink.Dispose();
        }

        // ─── suffix and overwrite tests ──────────────────────────────────────────

        [TestMethod]
        public void Suffix_None_UsesExactFileName()
        {
            var path = TempFile();
            Write(path, new[] { 1 });
            Assert.IsTrue(File.Exists(path));
        }

        [TestMethod]
        public void Overwrite_False_ThrowsWhenFileExists()
        {
            var path = TempFile();
            File.WriteAllText(path, "existing");
            var writer = new ParquetWriter { FileName = path, Overwrite = false };
            Assert.Throws<IOException>(() =>
                writer.Process(Observable.Return(1)).Wait());
        }

        [TestMethod]
        public void Overwrite_True_SucceedsWhenFileExists()
        {
            var path = TempFile();
            File.WriteAllText(path, "existing");
            Write(path, new[] { 42 }, overwrite: true);
            CollectionAssert.AreEqual(new[] { 42 }, ReadColumn<int>(path));
        }

        [TestMethod]
        public void Suffix_Timestamp_ProducesNewFileName()
        {
            var baseName = "bptest_" + Guid.NewGuid().ToString("N");
            var basePath = Path.Combine(Path.GetTempPath(), baseName + ".parquet");
            var writer = new ParquetWriter { FileName = basePath, Suffix = PathSuffix.Timestamp };
            writer.Process(Observable.Return(1)).Wait();

            var dir = Path.GetDirectoryName(basePath)!;
            var files = Directory.GetFiles(dir, baseName + "*.parquet");
            Assert.AreEqual(1, files.Length, "Expected exactly one file with timestamp suffix.");
            var actualPath = files[0];
            Assert.AreNotEqual(basePath, actualPath, "Timestamped file should differ from base path.");
            File.Delete(actualPath);
        }

        [TestMethod]
        public void Suffix_FileCount_ProducesNewFileName()
        {
            var baseName = "bptest_" + Guid.NewGuid().ToString("N");
            var basePath = Path.Combine(Path.GetTempPath(), baseName + ".parquet");
            var writer = new ParquetWriter { FileName = basePath, Suffix = PathSuffix.FileCount };
            writer.Process(Observable.Return(1)).Wait();

            var dir = Path.GetDirectoryName(basePath)!;
            var files = Directory.GetFiles(dir, baseName + "*.parquet");
            Assert.AreEqual(1, files.Length, "Expected exactly one file with file-count suffix.");
            File.Delete(files[0]);
        }

        // ─── unsupported type rejection tests ────────────────────────────────────

        [TestMethod]
        public void UnsupportedType_NestedObject_ThrowsAtSubscribeTime()
        {
            var path = TempFile();
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            // Nested object (not a flat projection) — should throw NotSupportedException at subscribe
            var rows = new[] { new NestedRow { Inner = new InnerObject { Value = 1 } } };
            Assert.Throws<NotSupportedException>(() =>
                writer.Process(rows.ToObservable()).Wait());
        }
    }

    // ─── test helpers and types ───────────────────────────────────────────────

    public enum TestEnum
    {
        A = 0,
        B = 1,
        C = 2
    }

    public sealed class NestedRow
    {
        public InnerObject? Inner { get; set; }
    }

    public sealed class InnerObject
    {
        public int Value { get; set; }
    }

    /// <summary>
    /// Exposes <see cref="ParquetSink{T}"/> internals for lifecycle testing.
    /// </summary>
    internal sealed class ParquetSinkTestHarness<T> : IDisposable
    {
        public ParquetSink<T> Sink { get; }

        public ParquetSinkTestHarness(string path)
        {
            Sink = new ParquetSink<T>(path, ParquetSharp.Compression.Snappy, 1000);
        }

        public void Dispose() => Sink.Dispose();
    }
}
