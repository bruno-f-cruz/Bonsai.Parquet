using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using global::Apache.Arrow;
using global::Apache.Arrow.Ipc;
using global::Apache.Arrow.Types;
using P = Bonsai.Apache.Parquet;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class ExplicitBatchWriterTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".data");

        [TestCleanup]
        public void Cleanup() => File.Delete(path);

        IDisposable CreateWriter(string format, out Action<int[]> write, out Action<IList<int[]>> writeBatch,
            Action<IList<int[]>>? inspect = null)
        {
            if (format == "Arrow")
            {
                var schema = new Schema(new[] { new Field("Value", Int32Type.Default, false) }, null);
                var writer = new ArrowBatchWriter<int[]>(path, schema, 2, (items, s) =>
                {
                    inspect?.Invoke(items);
                    var values = items.SelectMany(item => item).ToArray();
                    return new RecordBatch(s, new IArrowArray[] { new Int32Array.Builder().AppendRange(values).Build() }, values.Length);
                });
                write = writer.Write;
                writeBatch = writer.WriteBatch;
                return writer;
            }
            else
            {
                var writer = new P.ParquetBatchWriter<int[]>(path, new ParquetSharp.Column[] { new ParquetSharp.Column<int>("Value") }, 2,
                    (items, group) =>
                    {
                        inspect?.Invoke(items);
                        using var column = group.NextColumn().LogicalWriter<int>();
                        column.WriteBatch(items.SelectMany(item => item).ToArray());
                    });
                write = writer.Write;
                writeBatch = writer.WriteBatch;
                return writer;
            }
        }

        void Verify(string format, params int[][] expected)
        {
            var groups = new List<int[]>();
            if (format == "Arrow")
            {
                using var reader = new ArrowFileReader(File.OpenRead(path));
                while (true)
                {
                    using var batch = reader.ReadNextRecordBatch();
                    if (batch == null) break;
                    var values = (Int32Array)batch.Column(0);
                    groups.Add(Enumerable.Range(0, batch.Length).Select(i => values.GetValue(i)!.Value).ToArray());
                }
            }
            else
            {
                using var reader = new ParquetSharp.ParquetFileReader(path);
                for (int i = 0; i < reader.FileMetaData.NumRowGroups; i++)
                {
                    using var group = reader.RowGroup(i);
                    using var column = group.Column(0).LogicalReader<int>();
                    groups.Add(column.ReadAll((int)group.MetaData.NumRows));
                }
            }
            Assert.HasCount(expected.Length, groups);
            for (int i = 0; i < expected.Length; i++) CollectionAssert.AreEqual(expected[i], groups[i]);
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void ExplicitBatchesPreserveExternalBoundariesAndDoNotRetainTheInputList(string format)
        {
            var counts = new List<int>();
            var first = new List<int[]> { new[] { 1, 2 }, System.Array.Empty<int>(), new[] { 3 } };
            using (CreateWriter(format, out _, out var writeBatch, items => counts.Add(items.Count)))
            {
                writeBatch(first); // Three frames exceed the writer's two-item buffer.
                first.Clear();
                writeBatch(System.Array.AsReadOnly(new[] { new[] { 4, 5, 6, 7 } }));
            }
            CollectionAssert.AreEqual(new[] { 3, 1 }, counts);
            Verify(format, new[] { 1, 2, 3 }, new[] { 4, 5, 6, 7 });
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void MixingBufferedItemsAndExplicitBatchesPreservesOrder(string format)
        {
            using (CreateWriter(format, out var write, out var writeBatch))
            {
                write(new[] { 0 });
                writeBatch(new[] { new[] { 1 }, new[] { 2 }, new[] { 3 } });
                write(new[] { 4 });
            }
            Verify(format, new[] { 0 }, new[] { 1, 2, 3 }, new[] { 4 });
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void EmptyAndNullBatchesDoNotFlushPendingItems(string format)
        {
            int calls = 0;
            using (CreateWriter(format, out var write, out var writeBatch, _ => calls++))
            {
                write(new[] { 42 });
                writeBatch(System.Array.Empty<int[]>());
                Assert.ThrowsExactly<ArgumentNullException>(() => writeBatch(null!));
                Assert.AreEqual(0, calls);
            }
            Assert.AreEqual(1, calls);
            Verify(format, new[] { 42 });
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void ExplicitConversionFailureReachesTheCallerAndIsNotRetriedOnDispose(string format)
        {
            int calls = 0;
            var expected = new InvalidOperationException("conversion failed");
            using (CreateWriter(format, out _, out var writeBatch, _ => { calls++; throw expected; }))
                Assert.AreSame(expected, Assert.ThrowsExactly<InvalidOperationException>(() => writeBatch(new[] { new[] { 1 } })));
            Assert.AreEqual(1, calls);
            using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
        }
    }
}
