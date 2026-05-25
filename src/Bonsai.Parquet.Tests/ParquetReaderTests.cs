using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using Bonsai.Parquet;

namespace Bonsai.Parquet.Tests
{
    [TestClass]
    public sealed class ParquetReaderTests
    {
        // ─── helpers ────────────────────────────────────────────────────────────

        static string TempFile() =>
            Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".parquet");

        static void Write<T>(string path, IEnumerable<T> rows, int rowGroupSize = 100_000)
        {
            var writer = new ParquetWriter { FileName = path, Overwrite = true, RowGroupSize = rowGroupSize };
            writer.Process(rows.ToObservable()).DefaultIfEmpty().Wait();
        }

        // ─── concrete reader subclasses ──────────────────────────────────────────

        private class IntReader : AbstractParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int)) };

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int>("Value");
                return i => v[i];
            }
        }

        private class StringReader : AbstractParquetReader<string?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(string)) };

            protected override Func<int, string?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<string>("Value");
                return i => v[i];
            }
        }

        private class NullableIntReader : AbstractParquetReader<int?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int?)) };

            protected override Func<int, int?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int?>("Value");
                return i => v[i];
            }
        }

        private class IntArrayReader : AbstractParquetReader<int[]?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int[])) };

            protected override Func<int, int[]?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int[]>("Value");
                return i => v[i];
            }
        }

        private class TupleReader : AbstractParquetReader<Tuple<int, string?>>
        {
            protected override IReadOnlyList<ColumnBinding> Columns => new[]
            {
                new ColumnBinding("X", typeof(int)),
                new ColumnBinding("Label", typeof(string)),
            };

            protected override Func<int, Tuple<int, string?>> CreateRowFactory(IParquetBatch batch)
            {
                var xs = batch.Column<int>("X");
                var labels = batch.Column<string>("Label");
                return i => Tuple.Create(xs[i], labels[i]);
            }
        }

        [TestMethod]
        public void RoundTrip_Scalar()
        {
            var path = TempFile();
            Write(path, new[] { 1, -2, int.MaxValue });
            var result = new IntReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1, -2, int.MaxValue }, result);
        }

        [TestMethod]
        public void RoundTrip_VariableLength()
        {
            var path = TempFile();
            Write(path, new[] { "hello", "world", "" });
            var result = new StringReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { "hello", "world", "" }, result);
        }

        [TestMethod]
        public void RoundTrip_Nullable_WithNulls()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, null, 3 });
            var result = new NullableIntReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, result);
        }

        [TestMethod]
        public void RoundTrip_List()
        {
            var path = TempFile();
            Write(path, new[] { new[] { 1, 2, 3 }, new[] { 4, 5 } });
            var result = new IntArrayReader { FileName = path }.Generate().ToArray().Wait();
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
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            var result = new TupleReader { FileName = path }.Generate().ToArray().Wait();
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
            var small = new IntReader { FileName = path, BatchSize = 1 }.Generate().ToArray().Wait();
            var large = new IntReader { FileName = path, BatchSize = 1000 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(expected, small);
            CollectionAssert.AreEqual(expected, large);
        }

        [TestMethod]
        public void RowGroupSpanning_BatchSpansBoundaries_AllRowsInOrder()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 10), rowGroupSize: 3);
            var result = new IntReader { FileName = path, BatchSize = 4 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 10).ToArray(), result);
        }

        // ─── validation ──────────────────────────────────────────────────────────

        [TestMethod]
        public void Validation_MissingColumn_ThrowsWithColumnName()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 });

            var reader = new MissingColumnReader { FileName = path };
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate().ToArray().Wait());
            StringAssert.Contains(ex.Message, "NonExistentColumn");
        }

        [TestMethod]
        public void Validation_WrongType_ThrowsWithExpectedAndActual()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 });

            var reader = new WrongTypeReader { FileName = path };
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate().ToArray().Wait());
            StringAssert.Contains(ex.Message, "System.Double");
            StringAssert.Contains(ex.Message, "System.Int32");
        }

        [TestMethod]
        public void Validation_MissingFile_ThrowsFileNotFoundException()
        {
            var reader = new IntReader { FileName = @"C:\nonexistent_parquet_file_xyz.parquet" };
            Assert.Throws<FileNotFoundException>(() =>
                reader.Generate().ToArray().Wait());
        }

        // ─── empty file ──────────────────────────────────────────────────────────

        [TestMethod]
        public void EmptyFile_ProducesEmptySequence()
        {
            var path = TempFile();
            Write(path, Enumerable.Empty<int>());

            var result = new IntReader { FileName = path }.Generate().ToArray().Wait();
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
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            var result = new XOnlyReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 0, 1, 2 }, result);
        }

        // ─── subscription disposal mid-read ─────────────────────────────────────

        [TestMethod, Timeout(5000, CooperativeCancellation = true)]
        public void Disposal_MidRead_StopsCleanly()
        {
            var path = TempFile();
            Write(path, Enumerable.Range(0, 1000));

            var reader = new IntReader { FileName = path, BatchSize = 10 };
            int count = 0;
            var exceptions = new List<Exception>();
            IDisposable? sub = null;

            sub = reader.Generate().Subscribe(
                x =>
                {
                    if (Interlocked.Increment(ref count) >= 5)
                        sub?.Dispose();
                },
                ex => exceptions.Add(ex));

            Thread.Sleep(300);

            CollectionAssert.AreEqual(new List<Exception>(), exceptions,
                "No exceptions should be thrown during mid-read disposal.");
            Assert.IsLessThan(200,
count, $"Expected cancellation to stop emitting after ~5 items, but received {count}.");
        }

        // ─── validation reader helpers ────────────────────────────────────────────

        private class MissingColumnReader : AbstractParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("NonExistentColumn", typeof(int)) };

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int>("NonExistentColumn");
                return i => v[i];
            }
        }

        private class WrongTypeReader : AbstractParquetReader<double>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(double)) };

            protected override Func<int, double> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<double>("Value");
                return i => v[i];
            }
        }

        private class XOnlyReader : AbstractParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("X", typeof(int)) };

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int>("X");
                return i => v[i];
            }
        }
    }
}
