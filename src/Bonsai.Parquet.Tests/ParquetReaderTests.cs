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

        private class BoolReader : ParquetReader<bool>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(bool)) };

            protected override Func<int, bool> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<bool>("Value");
                return i => v[i];
            }
        }

        private class IntReader : ParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int)) };

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int>("Value");
                return i => v[i];
            }
        }

        private class LongReader : ParquetReader<long>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(long)) };

            protected override Func<int, long> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<long>("Value");
                return i => v[i];
            }
        }

        private class DoubleReader : ParquetReader<double>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(double)) };

            protected override Func<int, double> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<double>("Value");
                return i => v[i];
            }
        }

        private class StringReader : ParquetReader<string?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(string)) };

            protected override Func<int, string?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<string>("Value");
                return i => v[i];
            }
        }

        private class DateTimeReader : ParquetReader<DateTime>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(DateTime)) };

            protected override Func<int, DateTime> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<DateTime>("Value");
                return i => v[i];
            }
        }

        private class GuidReader : ParquetReader<Guid>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(Guid)) };

            protected override Func<int, Guid> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<Guid>("Value");
                return i => v[i];
            }
        }

        private class DecimalReader : ParquetReader<decimal>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(decimal)) };

            protected override Func<int, decimal> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<decimal>("Value");
                return i => v[i];
            }
        }

        private class ByteArrayReader : ParquetReader<byte[]?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(byte[])) };

            protected override Func<int, byte[]?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<byte[]>("Value");
                return i => v[i];
            }
        }

        private class NullableIntReader : ParquetReader<int?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int?)) };

            protected override Func<int, int?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int?>("Value");
                return i => v[i];
            }
        }

        private class IntArrayReader : ParquetReader<int[]?>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(int[])) };

            protected override Func<int, int[]?> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int[]>("Value");
                return i => v[i];
            }
        }

        private class TupleReader : ParquetReader<Tuple<int, string?>>
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

        // ─── round-trip Tier-1 type tests ───────────────────────────────────────

        [TestMethod]
        public void RoundTrip_Bool()
        {
            var path = TempFile();
            Write(path, new[] { true, false, true });
            var result = new BoolReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { true, false, true }, result);
        }

        [TestMethod]
        public void RoundTrip_Int()
        {
            var path = TempFile();
            Write(path, new[] { 1, -2, int.MaxValue });
            var result = new IntReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1, -2, int.MaxValue }, result);
        }

        [TestMethod]
        public void RoundTrip_Long()
        {
            var path = TempFile();
            Write(path, new[] { 0L, long.MinValue, long.MaxValue });
            var result = new LongReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 0L, long.MinValue, long.MaxValue }, result);
        }

        [TestMethod]
        public void RoundTrip_Double()
        {
            var path = TempFile();
            Write(path, new[] { 1.0, -2.5, double.PositiveInfinity });
            var result = new DoubleReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1.0, -2.5, double.PositiveInfinity }, result);
        }

        [TestMethod]
        public void RoundTrip_String()
        {
            var path = TempFile();
            Write(path, new[] { "hello", "world", "" });
            var result = new StringReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { "hello", "world", "" }, result);
        }

        [TestMethod]
        public void RoundTrip_DateTime()
        {
            var path = TempFile();
            var dt1 = new DateTime(2024, 1, 15, 10, 30, 0, 123, DateTimeKind.Unspecified);
            var dt2 = new DateTime(2000, 6, 1, 0, 0, 0, 0, DateTimeKind.Unspecified);
            Write(path, new[] { dt1, dt2 });
            var result = new DateTimeReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { dt1, dt2 }, result);
        }

        [TestMethod]
        public void RoundTrip_Guid()
        {
            var path = TempFile();
            var g1 = Guid.NewGuid();
            var g2 = Guid.Empty;
            Write(path, new[] { g1, g2 });
            var result = new GuidReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { g1, g2 }, result);
        }

        [TestMethod]
        public void RoundTrip_Decimal()
        {
            var path = TempFile();
            Write(path, new[] { 1.23m, -4.56m, 0m });
            var result = new DecimalReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 1.23m, -4.56m, 0m }, result);
        }

        [TestMethod]
        public void RoundTrip_ByteArray()
        {
            var path = TempFile();
            var data = new[] { new byte[] { 1, 2, 3 }, new byte[] { 4, 5 } };
            Write(path, data);
            var result = new ByteArrayReader { FileName = path }.Generate().ToArray().Wait();
            Assert.AreEqual(2, result.Length);
            CollectionAssert.AreEqual(data[0], result[0]);
            CollectionAssert.AreEqual(data[1], result[1]);
        }

        // ─── round-trip nullable ─────────────────────────────────────────────────

        [TestMethod]
        public void RoundTrip_NullableInt_WithNulls()
        {
            var path = TempFile();
            Write(path, new int?[] { 1, null, 3 });
            var result = new NullableIntReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new int?[] { 1, null, 3 }, result);
        }

        [TestMethod]
        public void RoundTrip_NullableInt_NonNull()
        {
            var path = TempFile();
            Write(path, new int?[] { 10, 20, 30 });
            var result = new NullableIntReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new int?[] { 10, 20, 30 }, result);
        }

        // ─── round-trip array column ─────────────────────────────────────────────

        [TestMethod]
        public void RoundTrip_IntArray()
        {
            var path = TempFile();
            Write(path, new[] { new[] { 1, 2, 3 }, new[] { 4, 5 } });
            var result = new IntArrayReader { FileName = path }.Generate().ToArray().Wait();
            Assert.AreEqual(2, result.Length);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, result[0]);
            CollectionAssert.AreEqual(new[] { 4, 5 }, result[1]);
        }

        // ─── multi-column ────────────────────────────────────────────────────────

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
            Assert.AreEqual(4, result.Length);
            for (int i = 0; i < 4; i++)
            {
                Assert.AreEqual(i, result[i].Item1);
                Assert.AreEqual(i.ToString(), result[i].Item2);
            }
        }

        // ─── BatchSize variations ────────────────────────────────────────────────

        [TestMethod]
        public void BatchSize_1_ProducesSameSequence()
        {
            var path = TempFile();
            var expected = Enumerable.Range(0, 10).ToArray();
            Write(path, expected);
            var result = new IntReader { FileName = path, BatchSize = 1 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BatchSize_3_ProducesSameSequence()
        {
            var path = TempFile();
            var expected = Enumerable.Range(0, 10).ToArray();
            Write(path, expected);
            var result = new IntReader { FileName = path, BatchSize = 3 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(expected, result);
        }

        [TestMethod]
        public void BatchSize_1000_ProducesSameSequence()
        {
            var path = TempFile();
            var expected = Enumerable.Range(0, 10).ToArray();
            Write(path, expected);
            var result = new IntReader { FileName = path, BatchSize = 1000 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(expected, result);
        }

        // ─── row-group spanning ──────────────────────────────────────────────────

        [TestMethod]
        public void RowGroupSpanning_ReadAcrossBoundaries_AllRowsInOrder()
        {
            var path = TempFile();
            // 10 rows, rowGroupSize=3 → groups of 3, 3, 3, 1 = 4 row groups
            Write(path, Enumerable.Range(0, 10), rowGroupSize: 3);
            // BatchSize=4 spans row-group boundaries
            var result = new IntReader { FileName = path, BatchSize = 4 }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 10).ToArray(), result);
        }

        // ─── validation ──────────────────────────────────────────────────────────

        [TestMethod]
        public void Validation_MissingColumn_ThrowsWithColumnName()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 }); // writes column "Value"

            var reader = new MissingColumnReader { FileName = path };
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate().ToArray().Wait());
            StringAssert.Contains(ex.Message, "NonExistentColumn");
        }

        [TestMethod]
        public void Validation_WrongType_ThrowsWithExpectedAndActual()
        {
            var path = TempFile();
            Write(path, new[] { 1, 2, 3 }); // writes int "Value"

            var reader = new WrongTypeReader { FileName = path };
            var ex = Assert.Throws<InvalidOperationException>(() =>
                reader.Generate().ToArray().Wait());
            // Message should contain both expected (System.Double) and actual (System.Int32)
            StringAssert.Contains(ex.Message, "System.Double");
            StringAssert.Contains(ex.Message, "System.Int32");
        }

        [TestMethod]
        public void Validation_EmptyColumns_ThrowsInvalidOperation()
        {
            var path = TempFile();
            Write(path, new[] { 1 });
            var reader = new EmptyColumnsReader { FileName = path };
            Assert.Throws<InvalidOperationException>(() =>
                reader.Generate().ToArray().Wait());
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

            var completed = false;
            var result = new List<int>();
            new IntReader { FileName = path }
                .Generate()
                .Do(_ => { }, () => completed = true)
                .ToArray().Wait();

            Assert.AreEqual(0, result.Count);
            Assert.IsTrue(completed);
        }

        // ─── extra columns in file are ignored ───────────────────────────────────

        [TestMethod]
        public void ExtraColumnsInFile_AreIgnored()
        {
            var path = TempFile();
            // Write two columns: X (int) and Label (string)
            var rows = Enumerable.Range(0, 3)
                .Select(i => new { X = i, Label = i.ToString() })
                .ToObservable();
            var writer = new ParquetWriter { FileName = path, Overwrite = true };
            writer.Process(rows).Wait();

            // Read only the X column — Label column in file should be ignored, no error
            var result = new XOnlyReader { FileName = path }.Generate().ToArray().Wait();
            CollectionAssert.AreEqual(new[] { 0, 1, 2 }, result);
        }

        // ─── subscription disposal mid-read ─────────────────────────────────────

        [TestMethod, Timeout(5000)]
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

            // Give the background thread time to wind down after cancellation.
            Thread.Sleep(300);

            CollectionAssert.AreEqual(new List<Exception>(), exceptions,
                "No exceptions should be thrown during mid-read disposal.");
            Assert.IsTrue(count < 200,
                $"Expected cancellation to stop emitting after ~5 items, but received {count}.");
        }

        // ─── validation reader helpers ────────────────────────────────────────────

        private class MissingColumnReader : ParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("NonExistentColumn", typeof(int)) };

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<int>("NonExistentColumn");
                return i => v[i];
            }
        }

        private class WrongTypeReader : ParquetReader<double>
        {
            // Declares double, but file has int
            protected override IReadOnlyList<ColumnBinding> Columns =>
                new[] { new ColumnBinding("Value", typeof(double)) };

            protected override Func<int, double> CreateRowFactory(IParquetBatch batch)
            {
                var v = batch.Column<double>("Value");
                return i => v[i];
            }
        }

        private class EmptyColumnsReader : ParquetReader<int>
        {
            protected override IReadOnlyList<ColumnBinding> Columns =>
                Array.Empty<ColumnBinding>();

            protected override Func<int, int> CreateRowFactory(IParquetBatch batch) => _ => 0;
        }

        private class XOnlyReader : ParquetReader<int>
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
