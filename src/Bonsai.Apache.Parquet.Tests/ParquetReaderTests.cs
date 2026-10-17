using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using Bonsai.Apache.Parquet;

namespace Bonsai.Apache.Parquet.Tests
{
    [TestClass]
    public sealed class ParquetReaderTests
    {
        // ─── helpers ────────────────────────────────────────────────────────────

        static string TempFile() =>
            Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".parquet");

        static void Write<T>(string path, IEnumerable<T> rows, int rowGroupSize = 100_000)
        {
            var writer = new ParquetWriter { Buffered = false, FileName = path, Overwrite = true, RowGroupSize = rowGroupSize };
            writer.Process(rows.ToObservable()).DefaultIfEmpty().Wait();
        }

        static ParquetReader Reader(string path, ParquetColumnType type = ParquetColumnType.Int32,
            string name = "Value", bool nullable = false, bool array = false, int batchSize = 4096)
        {
            var reader = new ParquetReader { FileName = path, BatchSize = batchSize };
            reader.Columns.Add(new ColumnDefinition { Name = name, Type = type, IsNullable = nullable, IsArray = array });
            return reader;
        }

        [TestMethod]
        public void RoundTrip_Scalar()
        {
            var path = TempFile();
            Write(path, new[] { 1, -2, int.MaxValue });
            var result = Reader(path).Generate<int>().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1, -2, int.MaxValue }, result);
        }

        [TestMethod]
        public void RoundTrip_VariableLength()
        {
            var path = TempFile();
            Write(path, new[] { "hello", "world", "" });
            var result = Reader(path, type: ParquetColumnType.String).Generate<string?>().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { "hello", "world", "" }, result);
        }

        [TestMethod]
        public void RoundTrip_Nullable_WithNulls()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, null, 3 });
            var result = Reader(path, nullable: true).Generate<int?>().ToArray().Wait();
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, result);
        }

        [TestMethod]
        public void RoundTrip_List()
        {
            var path = TempFile();
            Write(path, new[] { new[] { 1, 2, 3 }, new[] { 4, 5 } });
            var result = Reader(path, array: true).Generate<int[]?>().ToArray().Wait();
            Assert.HasCount(2, result);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, result[0]);
            CollectionAssert.AreEqual(new[] { 4, 5 }, result[1]);
        }

        [TestMethod]
        public void RoundTrip_MultiColumn_Tuple()
        {
            var path = TempFile();
            var rows = Enumerable.Range(0, 4)
                .Select(i => new { X = i, Label = i.ToString() })
                .ToObservable();
            var writer = new ParquetWriter { Buffered = false, FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            var reader = Reader(path, name: "X");
            reader.Columns.Add(new ColumnDefinition { Name = "Label", Type = ParquetColumnType.String });
            var result = reader.Generate<Tuple<int, string?>>().ToArray().Wait();
            Assert.HasCount(4, result);
            for (int i = 0; i < 4; i++)
            {
                Assert.AreEqual(i, result[i].Item1);
                Assert.AreEqual(i.ToString(), result[i].Item2);
            }
        }

        // ─── batch / row-group plumbing ──────────────────────────────────────────

        [TestMethod]
        public void BatchSize_DoesNotAffectOutput()
        {
            var path = TempFile();
            var expected = Enumerable.Range(0, 10).ToArray();
            Write(path, expected);
            var small = Reader(path, batchSize: 1).Generate<int>().ToArray().Wait();
            var large = Reader(path, batchSize: 1000).Generate<int>().ToArray().Wait();
            CollectionAssert.AreEqual(expected, small);
            CollectionAssert.AreEqual(expected, large);
        }

        [TestMethod]
        public void RowGroupSpanning_BatchSpansBoundaries_AllRowsInOrder()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 10), rowGroupSize: 3);
            var result = Reader(path, batchSize: 4).Generate<int>().ToArray().Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 10).ToArray(), result);
        }

        // ─── validation ──────────────────────────────────────────────────────────

        [TestMethod]
        public void Validation_MissingColumn_ThrowsWithColumnName()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 });

            var reader = Reader(path, name: "NonExistentColumn");
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate<int>().ToArray().Wait());
            StringAssert.Contains(ex.Message, "NonExistentColumn");
        }

        [TestMethod]
        public void Validation_WrongType_ThrowsWithExpectedAndActual()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 });

            var reader = Reader(path, type: ParquetColumnType.Double);
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate<double>().ToArray().Wait());
            StringAssert.Contains(ex.Message, "System.Double");
            StringAssert.Contains(ex.Message, "System.Int32");
        }

        [TestMethod]
        public void Validation_MissingFile_ThrowsFileNotFoundException()
        {
            var reader = Reader(@"C:\nonexistent_parquet_file_xyz.parquet");
            Assert.Throws<FileNotFoundException>(() =>
                reader.Generate<int>().ToArray().Wait());
        }

        // ─── empty file ──────────────────────────────────────────────────────────

        [TestMethod]
        public void EmptyFile_ProducesEmptySequence()
        {
            var path = TempFile();
            Write(path, Enumerable.Empty<int>());

            var result = Reader(path).Generate<int>().ToArray().Wait();
            Assert.IsEmpty(result);
        }

        // ─── extra columns in file are ignored ───────────────────────────────────

        [TestMethod]
        public void ExtraColumnsInFile_AreIgnored()
        {
            var path = TempFile();
            var rows = Enumerable.Range(0, 3)
                .Select(i => new { X = i, Label = i.ToString() })
                .ToObservable();
            var writer = new ParquetWriter { Buffered = false, FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            var result = Reader(path, name: "X").Generate<int>().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 0, 1, 2 }, result);
        }

        // ─── subscription disposal mid-read ─────────────────────────────────────

        [TestMethod, Timeout(5000, CooperativeCancellation = true)]
        public void Disposal_MidRead_StopsCleanly()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 1000));

            var reader = Reader(path, batchSize: 10);
            var result = reader.Generate<int>().Take(5).ToArray().Timeout(TimeSpan.FromSeconds(3)).Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 5).ToArray(), result);
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                try { using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None); return true; }
                catch (IOException) { return false; }
            }, TimeSpan.FromSeconds(1)), "Cancellation must release the input file.");
        }

    }
}
