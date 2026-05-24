using System;
using System.Collections.Generic;
using System.Reflection;
using ParquetSharp;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Manages a single open Parquet file, staging rows into per-column buffers and flushing row groups.
    /// </summary>
    internal sealed class ParquetSink<TRow> : IDisposable
    {
        readonly ParquetFileWriter _fileWriter;
        readonly ColumnPlan[] _plan;
        readonly int _rowGroupSize;
        readonly System.Collections.IList[] _buffers;
        int _bufferedRows;
        bool _disposed;

        internal ParquetSink(string path, ColumnPlan[] plan, Compression compression, int rowGroupSize)
        {
            _plan = plan;
            _rowGroupSize = rowGroupSize;
            var columns = new Column[plan.Length];
            for (int i = 0; i < plan.Length; i++) columns[i] = plan[i].Column;
            _fileWriter = new ParquetFileWriter(path, columns, compression);
            _buffers = CreateBuffers();
        }

        internal void AppendRow(TRow row)
        {
            for (int i = 0; i < _plan.Length; i++)
            {
                _buffers[i].Add(_plan[i].Accessor(row!));
            }

            _bufferedRows++;
            if (_bufferedRows >= _rowGroupSize)
            {
                FlushRowGroup();
            }
        }

        void FlushRowGroup()
        {
            if (_bufferedRows == 0) return;

            using var rowGroupWriter = _fileWriter.AppendRowGroup();
            for (int i = 0; i < _plan.Length; i++)
            {
                using var colWriter = rowGroupWriter.NextColumn();
                WriteColumnDynamic(colWriter, _plan[i].LogicalSystemType, _buffers[i]);
            }

            foreach (var buf in _buffers)
            {
                buf.Clear();
            }
            _bufferedRows = 0;
        }

        static void WriteColumnDynamic(ColumnWriter colWriter, Type logicalType, System.Collections.IList buffer)
        {
            typeof(ParquetSink<TRow>)
                .GetMethod(nameof(WriteColumnGeneric), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(logicalType)
                .Invoke(null, new object[] { colWriter, buffer });
        }

        static void WriteColumnGeneric<TElement>(ColumnWriter colWriter, System.Collections.IList buffer)
        {
            var typed = (List<TElement>)buffer;
            using var writer = colWriter.LogicalWriter<TElement>();
            writer.WriteBatch(typed.ToArray());
        }

        System.Collections.IList[] CreateBuffers()
        {
            var buffers = new System.Collections.IList[_plan.Length];
            for (int i = 0; i < _plan.Length; i++)
            {
                var listType = typeof(List<>).MakeGenericType(_plan[i].LogicalSystemType);
                buffers[i] = (System.Collections.IList)Activator.CreateInstance(listType)!;
            }
            return buffers;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                if (_bufferedRows > 0)
                {
                    FlushRowGroup();
                }
                _fileWriter.Close();
            }
            finally
            {
                _fileWriter.Dispose();
            }
        }
    }
}
