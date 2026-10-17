using System;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    internal abstract class ColumnDecoder : IDisposable
    {
        public abstract void Read(int count);
        public abstract void Dispose();

        internal static ColumnDecoder Open<T>(RowGroupReader rowGroup, int columnIndex, T[] buffer)
        {
            var reader = rowGroup.Column(columnIndex).LogicalReader<T>();
            return new TypedDecoder<T>(reader, buffer);
        }

        private sealed class TypedDecoder<T> : ColumnDecoder
        {
            readonly LogicalColumnReader<T> _reader;
            readonly T[] _buffer;

            internal TypedDecoder(LogicalColumnReader<T> reader, T[] buffer)
            {
                _reader = reader;
                _buffer = buffer;
            }

            public override void Read(int count) => _reader.ReadBatch(_buffer, 0, count);

            public override void Dispose() => _reader.Dispose();
        }
    }
}
