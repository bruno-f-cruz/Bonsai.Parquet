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

        [TestMethod]
        public void RoundTrip_Scalar()
        {
            var path = TempFile();
            Write(path, new[] { 1, -2, int.MaxValue });
            CollectionAssert.AreEqual(new[] { 1, -2, int.MaxValue }, ReadColumn<int>(path));
        }

        [TestMethod]
        public void RoundTrip_VariableLength()
        {
            var path = TempFile();
            Write(path, new[] { "hello", "world", "" });
            CollectionAssert.AreEqual(new[] { "hello", "world", "" }, ReadColumn<string?>(path));
        }

        [TestMethod]
        public void RoundTrip_Nullable_WithNull()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, null, 3 });
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, ReadColumn<int?>(path));
        }

        [TestMethod]
        public void RoundTrip_List()
        {
            var path = TempFile();
            Write(path, new[] { new[] { 1, 2, 3 }, new[] { 4, 5 } });
            var actual = ReadColumn<int[]?>(path);
            Assert.AreEqual(2, actual.Length);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, actual[0]);
            CollectionAssert.AreEqual(new[] { 4, 5 }, actual[1]);
        }

        [TestMethod]
        public void RoundTrip_Enum_StoredAsUnderlyingInt()
        {
            var path = TempFile();
            Write(path, new[] { TestEnum.A, TestEnum.B, TestEnum.C });
            CollectionAssert.AreEqual(
                new[] { (int)TestEnum.A, (int)TestEnum.B, (int)TestEnum.C },
                ReadColumn<int>(path));
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

        // ─── row-group flushing tests ───────────────────────────────────────────

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
        public void RowGroup_MoreThanRowGroupSize_ProducesMultipleRowGroups()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 6), rowGroupSize: 5);
            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(2, reader.FileMetaData.NumRowGroups);
            using (var rg0 = reader.RowGroup(0)) Assert.AreEqual(5L, rg0.MetaData.NumRows);
            using (var rg1 = reader.RowGroup(1)) Assert.AreEqual(1L, rg1.MetaData.NumRows);
        }

        // ─── lifecycle tests ─────────────────────────────────────────────────────

        [TestMethod]
        public void Lifecycle_NonEmptyBuffer_FlushesOnDispose()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 }, rowGroupSize: 1000);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, ReadColumn<int>(path));
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
            sink.Sink.Dispose();
        }

        // ─── suffix and overwrite tests ──────────────────────────────────────────

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
        public void Suffix_Timestamp_ProducesSuffixedFileName()
        {
            var baseName = "bptest_" + Guid.NewGuid().ToString("N");
            var basePath = Path.Combine(Path.GetTempPath(), baseName + ".parquet");
            var writer = new ParquetWriter { FileName = basePath, Suffix = PathSuffix.Timestamp };
            writer.Process(Observable.Return(1)).Wait();

            var dir = Path.GetDirectoryName(basePath)!;
            var files = Directory.GetFiles(dir, baseName + "*.parquet");
            Assert.AreEqual(1, files.Length);
            Assert.AreNotEqual(basePath, files[0], "Timestamped file should differ from base path.");
            File.Delete(files[0]);
        }

        // ─── unsupported type rejection ──────────────────────────────────────────

        [TestMethod]
        public void UnsupportedType_NestedObject_ThrowsAtSubscribeTime()
        {
            var path = TempFile();
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            var rows = new[] { new NestedRow { Inner = new InnerObject { Value = 1 } } };
            Assert.Throws<NotSupportedException>(() =>
                writer.Process(rows.ToObservable()).Wait());
        }

        // ─── Selector tests ─────────────────────────────────────────────────────

        [TestMethod]
        public void Selector_PicksSubsetOfTopLevelMembers()
        {
            var path = TempFile();
            var writer = new ParquetWriter
            {
                FileName = path,
                Overwrite = true,
                Selector = "X,Z",
            };
            var rows = new[]
            {
                new Point3D { X = 1.0, Y = 2.0, Z = 3.0 },
                new Point3D { X = 4.0, Y = 5.0, Z = 6.0 },
            };
            writer.Process(rows.ToObservable()).Wait();

            using var reader = new ParquetFileReader(path);
            var schema = reader.FileMetaData.Schema;
            Assert.AreEqual(2, schema.NumColumns);
            Assert.AreEqual("X", schema.ColumnRoot(0).Name);
            Assert.AreEqual("Z", schema.ColumnRoot(1).Name);

            CollectionAssert.AreEqual(new[] { 1.0, 4.0 }, ReadColumn<double>(path, 0));
            CollectionAssert.AreEqual(new[] { 3.0, 6.0 }, ReadColumn<double>(path, 1));
        }

        [TestMethod]
        public void Selector_NestedPath_UsesDottedColumnName()
        {
            var path = TempFile();
            var writer = new ParquetWriter
            {
                FileName = path,
                Overwrite = true,
                Selector = "Inner.Value",
            };
            var rows = new[]
            {
                new NestedRow { Inner = new InnerObject { Value = 10 } },
                new NestedRow { Inner = new InnerObject { Value = 20 } },
            };
            writer.Process(rows.ToObservable()).Wait();

            using var reader = new ParquetFileReader(path);
            var schema = reader.FileMetaData.Schema;
            Assert.AreEqual(1, schema.NumColumns);
            Assert.AreEqual("Inner.Value", schema.ColumnRoot(0).Name);
            CollectionAssert.AreEqual(new[] { 10, 20 }, ReadColumn<int>(path));
        }

        [TestMethod]
        public void Selector_Empty_FallsBackToDefaultLayout()
        {
            var path = TempFile();
            var writer = new ParquetWriter
            {
                FileName = path,
                Overwrite = true,
                Selector = "",
            };
            var rows = new[] { new Point3D { X = 1, Y = 2, Z = 3 } };
            writer.Process(rows.ToObservable()).Wait();

            using var reader = new ParquetFileReader(path);
            Assert.AreEqual(3, reader.FileMetaData.Schema.NumColumns);
        }

        [TestMethod]
        public void Selector_InvalidPath_ThrowsAtSubscribeTime()
        {
            var path = TempFile();
            var writer = new ParquetWriter
            {
                FileName = path,
                Overwrite = true,
                Selector = "NotAMember",
            };
            var rows = new[] { new Point3D() };
            Assert.ThrowsExactly<InvalidOperationException>(() =>
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

    public sealed class Point3D
    {
        public double X { get; set; }
        public double Y { get; set; }
        public double Z { get; set; }
    }

    /// <summary>
    /// Exposes <see cref="ParquetSink{T}"/> internals for lifecycle testing.
    /// </summary>
    internal sealed class ParquetSinkTestHarness<T> : IDisposable
    {
        public ParquetSink<T> Sink { get; }

        public ParquetSinkTestHarness(string path)
        {
            var plan = SchemaInference.BuildPlan(typeof(T), selector: null);
            Sink = new ParquetSink<T>(path, plan, ParquetSharp.Compression.Snappy, 1000);
        }

        public void Dispose() => Sink.Dispose();
    }
}
