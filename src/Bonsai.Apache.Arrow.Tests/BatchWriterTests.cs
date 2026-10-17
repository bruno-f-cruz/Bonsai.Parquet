using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using global::Apache.Arrow;
using global::Apache.Arrow.Types;
using A = Bonsai.Apache.Arrow;
using P = Bonsai.Apache.Parquet;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class BatchWriterTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".data");

        [TestCleanup]
        public void Cleanup() => File.Delete(path);

        int[] Read(string format, string name = "Value")
        {
            if (format == "Arrow")
            {
                var reader = new A.ArrowReader { FileName = path, BatchSize = 2 };
                reader.Columns.Add(new A.ColumnDefinition { Name = name, Type = A.ArrowColumnType.Int32 });
                return reader.Generate<int>().ToArray().Wait();
            }
            var parquet = new P.ParquetReader { FileName = path, BatchSize = 2 };
            parquet.Columns.Add(new P.ColumnDefinition { Name = name, Type = P.ParquetColumnType.Int32 });
            return parquet.Generate<int>().ToArray().Wait();
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void InferredWriterFlushesFullAndPartialBatches(string format)
        {
            if (format == "Arrow")
            {
                using var writer = new A.ArrowBatchWriter<int>(path, 2, enableCompression: true);
                writer.Write(1); writer.Write(2); writer.Flush(); writer.Write(3);
            }
            else
            {
                using var writer = new P.ParquetBatchWriter<int>(path, 2, ParquetSharp.Compression.Zstd);
                writer.Write(1); writer.Write(2); writer.Flush(); writer.Write(3);
            }
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, Read(format));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void InferredWriterSupportsMemberSelection(string format)
        {
            if (format == "Arrow")
            {
                using var writer = new A.ArrowBatchWriter<Tuple<int, int>>(path, 2, selector: "Item2");
                writer.Write(Tuple.Create(1, 42));
            }
            else
            {
                using var writer = new P.ParquetBatchWriter<Tuple<int, int>>(path, 2, selector: "Item2");
                writer.Write(Tuple.Create(1, 42));
            }
            CollectionAssert.AreEqual(new[] { 42 }, Read(format, "Item2"));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void CustomConverterCanExpandInputItemsIntoRows(string format)
        {
            int calls = 0;
            if (format == "Arrow")
            {
                var schema = new Schema(new[] { new Field("Value", Int32Type.Default, false) }, null);
                using var writer = new A.ArrowBatchWriter<int[]>(path, schema, 2, (items, s) =>
                {
                    calls++;
                    var values = items.SelectMany(item => item).ToArray();
                    return new RecordBatch(s, new IArrowArray[] { new Int32Array.Builder().AppendRange(values).Build() }, values.Length);
                });
                writer.Write(new[] { 1, 2 }); writer.Write(new[] { 3 }); writer.Write(new[] { 4, 5 });
            }
            else
            {
                using var writer = new P.ParquetBatchWriter<int[]>(path, new ParquetSharp.Column[] { new ParquetSharp.Column<int>("Value") }, 2,
                    (items, group) =>
                    {
                        calls++;
                        using var column = group.NextColumn().LogicalWriter<int>();
                        column.WriteBatch(items.SelectMany(item => item).ToArray());
                    });
                writer.Write(new[] { 1, 2 }); writer.Write(new[] { 3 }); writer.Write(new[] { 4, 5 });
            }
            Assert.AreEqual(2, calls);
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5 }, Read(format));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void InvalidBufferSizeLeavesExistingFileUntouched(string format)
        {
            File.WriteAllText(path, "original");
            Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            {
                if (format == "Arrow") { using var writer = new A.ArrowBatchWriter<int>(path, 0); }
                else { using var writer = new P.ParquetBatchWriter<int>(path, 0); }
            });
            Assert.AreEqual("original", File.ReadAllText(path));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void DisposedWriterRejectsFurtherWritesAndCanBeDisposedTwice(string format)
        {
            if (format == "Arrow")
            {
                var writer = new A.ArrowBatchWriter<int>(path, 2);
                writer.Write(1); writer.Dispose(); writer.Dispose();
                Assert.ThrowsExactly<ObjectDisposedException>(() => writer.Write(2));
                Assert.ThrowsExactly<ObjectDisposedException>(writer.Flush);
            }
            else
            {
                var writer = new P.ParquetBatchWriter<int>(path, 2);
                writer.Write(1); writer.Dispose(); writer.Dispose();
                Assert.ThrowsExactly<ObjectDisposedException>(() => writer.Write(2));
                Assert.ThrowsExactly<ObjectDisposedException>(writer.Flush);
            }
            CollectionAssert.AreEqual(new[] { 1 }, Read(format));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void FailedCustomBatchIsNotRetriedDuringDisposal(string format)
        {
            int calls = 0;
            var expected = new InvalidOperationException("conversion failed");
            if (format == "Arrow")
            {
                var schema = new Schema(new[] { new Field("Value", Int32Type.Default, false) }, null);
                var writer = new A.ArrowBatchWriter<int>(path, schema, 10, (items, s) => { calls++; throw expected; });
                writer.Write(1);
                Assert.AreSame(expected, Assert.ThrowsExactly<InvalidOperationException>(writer.Flush));
                writer.Dispose(); writer.Dispose();
            }
            else
            {
                var writer = new P.ParquetBatchWriter<int>(path, new ParquetSharp.Column[] { new ParquetSharp.Column<int>("Value") }, 10,
                    (items, group) => { calls++; throw expected; });
                writer.Write(1);
                Assert.AreSame(expected, Assert.ThrowsExactly<InvalidOperationException>(writer.Flush));
                writer.Dispose(); writer.Dispose();
            }
            Assert.AreEqual(1, calls);
        }

        sealed class MutableRow
        {
            public int Value { get; set; }
            public int Calls;
            public bool Fail;
            public int Captured { get { Calls++; return Fail ? throw new InvalidOperationException("getter failed") : Value; } }
        }

        IDisposable CreateInferredWriter(string format, string selector, out Action<MutableRow> write)
        {
            if (format == "Arrow")
            {
                var writer = new A.ArrowBatchWriter<MutableRow>(path, 100, selector: selector);
                write = writer.Write;
                return writer;
            }
            else
            {
                var writer = new P.ParquetBatchWriter<MutableRow>(path, 100, selector: selector);
                write = writer.Write;
                return writer;
            }
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void ReusingAnInputObjectPreservesEachValueAtWriteTime(string format)
        {
            var row = new MutableRow { Value = 1 };
            using (CreateInferredWriter(format, "Captured", out var write))
            {
                write(row);
                Assert.AreEqual(1, row.Calls, "The getter must run during Write.");
                row.Value = 2;
                write(row);
                row.Value = 99;
            }
            Assert.AreEqual(2, row.Calls, "Flushing must not evaluate getters again.");
            CollectionAssert.AreEqual(new[] { 1, 2 }, Read(format, "Captured"));
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void GetterFailureDiscardsOnlyThePartialRowAndAllowsFurtherWrites(string format)
        {
            using (CreateInferredWriter(format, "Value,Captured", out var write))
            {
                write(new MutableRow { Value = 1 });
                Assert.ThrowsExactly<InvalidOperationException>(() => write(new MutableRow { Value = 2, Fail = true }));
                write(new MutableRow { Value = 3 });
            }
            CollectionAssert.AreEqual(new[] { 1, 3 }, Read(format, "Value"));
            CollectionAssert.AreEqual(new[] { 1, 3 }, Read(format, "Captured"));
        }
    }
}
