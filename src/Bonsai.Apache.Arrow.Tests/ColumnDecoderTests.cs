using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using global::Apache.Arrow.Ipc;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class ColumnDecoderTests
    {
        [TestMethod]
        public void SlicedBatchesPreserveScalarNullAndBinaryValuesAcrossDecodeChunks()
        {
            var original = Path.GetTempFileName();
            var sliced = Path.GetTempFileName();
            try
            {
                var rows = Enumerable.Range(0, 16).Select(i => new
                {
                    Index = i,
                    Optional = i % 2 == 0 ? (int?)null : i,
                    Data = new byte[] { (byte)i, 255 }
                }).ToArray();
                new ArrowWriter { FileName = original, Overwrite = true, Buffered = false, BatchSize = 16 }
                    .Process(rows.ToObservable()).Wait();
                using (var file = new ArrowFileReader(File.OpenRead(original)))
                using (var batch = file.ReadNextRecordBatch())
                using (var slice = batch.SliceShared(3, 9))
                    new ArrowWriter { FileName = sliced, Overwrite = true, Buffered = false }
                        .Process(Observable.Return(slice)).Wait();
                var reader = new ArrowReader { FileName = sliced, BatchSize = 2 };
                reader.Columns.Add(new ColumnDefinition { Name = "Index", Type = ArrowColumnType.Int32 });
                reader.Columns.Add(new ColumnDefinition { Name = "Optional", Type = ArrowColumnType.Int32, IsNullable = true });
                reader.Columns.Add(new ColumnDefinition { Name = "Data", Type = ArrowColumnType.Bytes });
                var actual = reader.Generate<Tuple<int, int?, byte[]>>().ToArray().Wait();
                Assert.HasCount(9, actual);
                for (int i = 0; i < actual.Length; i++)
                {
                    Assert.AreEqual(rows[i + 3].Index, actual[i].Item1);
                    Assert.AreEqual(rows[i + 3].Optional, actual[i].Item2);
                    CollectionAssert.AreEqual(rows[i + 3].Data, actual[i].Item3);
                }
            }
            finally { File.Delete(original); File.Delete(sliced); }
        }

        [TestMethod]
        public void NullCannotBeDecodedAsANonNullableScalar()
        {
            var path = Path.GetTempFileName();
            try
            {
                using (var writer = new ArrowBatchWriter<int?>(path, 2)) { writer.Write(1); writer.Write(null); }
                var reader = new ArrowReader { FileName = path };
                reader.Columns.Add(new ColumnDefinition { Name = "Value", Type = ArrowColumnType.Int32 });
                Assert.ThrowsExactly<InvalidOperationException>(() => reader.Generate<int>().Wait());
            }
            finally { File.Delete(path); }
        }
    }
}
