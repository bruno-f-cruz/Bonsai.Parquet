using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class ReaderIntegrationTests
    {
        string path = null!;

        [TestInitialize]
        public void Initialize() => path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".data");

        [TestCleanup]
        public void Cleanup() => File.Delete(path);

        void Write(string format)
        {
            var source = Enumerable.Range(0, 17).ToObservable();
            if (format == "Arrow")
                new ArrowWriter { FileName = path, Buffered = false, BatchSize = 5 }.Process(source).Wait();
            else
                new Parquet.ParquetWriter { FileName = path, Buffered = false, RowGroupSize = 5 }.Process(source).Wait();
        }

        IObservable<int> Read(string format, int batchSize, string name = "Value", bool duplicate = false)
        {
            if (format == "Arrow")
            {
                var reader = new ArrowReader { FileName = path, BatchSize = batchSize };
                reader.Columns.Add(new ColumnDefinition { Name = name, Type = ArrowColumnType.Int32 });
                if (duplicate) reader.Columns.Add(new ColumnDefinition { Name = name, Type = ArrowColumnType.Int32 });
                return reader.Generate<int>();
            }
            else
            {
                var reader = new Parquet.ParquetReader { FileName = path, BatchSize = batchSize };
                reader.Columns.Add(new Parquet.ColumnDefinition { Name = name, Type = Parquet.ParquetColumnType.Int32 });
                if (duplicate) reader.Columns.Add(new Parquet.ColumnDefinition { Name = name, Type = Parquet.ParquetColumnType.Int32 });
                return reader.Generate<int>();
            }
        }

        [TestMethod]
        [DataRow("Arrow", 1)]
        [DataRow("Arrow", 4)]
        [DataRow("Arrow", 4096)]
        [DataRow("Parquet", 1)]
        [DataRow("Parquet", 4)]
        [DataRow("Parquet", 4096)]
        public void DecodeSizePreservesRowsAcrossFileBatchesAndClosesBeforeCompletion(string format, int batchSize)
        {
            Write(format);
            var actual = Read(format, batchSize).Do(_ => { }, () =>
            {
                using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);
            }).ToArray().Timeout(TimeSpan.FromSeconds(5)).Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 17).ToArray(), actual);
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void CancellationStopsRowsAndReleasesTheFile(string format)
        {
            Write(format);
            var actual = Read(format, 4).Take(3).ToArray().Timeout(TimeSpan.FromSeconds(5)).Wait();
            CollectionAssert.AreEqual(new[] { 0, 1, 2 }, actual);
            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                try { using var file = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None); return true; }
                catch (IOException) { return false; }
            }, TimeSpan.FromSeconds(2)));
        }

        [TestMethod]
        [DataRow("Arrow", 0)]
        [DataRow("Arrow", -1)]
        [DataRow("Parquet", 0)]
        [DataRow("Parquet", -1)]
        public void InvalidDecodeSizeReportsAnError(string format, int batchSize)
        {
            Write(format);
            var error = Assert.ThrowsExactly<InvalidOperationException>(() => Read(format, batchSize).Wait());
            StringAssert.Contains(error.Message, "BatchSize");
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void DuplicateColumnsReportAnError(string format)
        {
            Write(format);
            var error = Assert.ThrowsExactly<InvalidOperationException>(() => Read(format, 4, duplicate: true).Wait());
            StringAssert.Contains(error.Message, "unique");
        }

        [TestMethod]
        [DataRow("Arrow")]
        [DataRow("Parquet")]
        public void MissingColumnReportsItsName(string format)
        {
            Write(format);
            var error = Assert.ThrowsExactly<InvalidOperationException>(() => Read(format, 4, "Missing").Wait());
            StringAssert.Contains(error.Message, "Missing");
        }

        [TestMethod]
        [DataRow("Arrow", false)]
        [DataRow("Arrow", true)]
        [DataRow("Parquet", false)]
        [DataRow("Parquet", true)]
        public void TypeMismatchIsRejectedForBothEmptyAndPopulatedFiles(string format, bool empty)
        {
            var source = empty ? Observable.Empty<double>() : Observable.Return(1.25);
            if (format == "Arrow")
                new ArrowWriter { FileName = path, Buffered = false }.Process(source).DefaultIfEmpty().Wait();
            else
                new Parquet.ParquetWriter { FileName = path, Buffered = false }.Process(source).DefaultIfEmpty().Wait();
            var error = Assert.ThrowsExactly<InvalidOperationException>(() => Read(format, 4).Wait());
            StringAssert.Contains(error.Message, "Value");
        }
    }
}
