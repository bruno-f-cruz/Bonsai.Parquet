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
        readonly Column[] _columns;
        readonly ColumnAccessor[] _accessors;
        readonly int _rowGroupSize;
        readonly System.Collections.IList[] _buffers;
        int _bufferedRows;
        bool _disposed;

        internal ParquetSink(string path, Compression compression, int rowGroupSize)
        {
            _columns = SchemaInference.InferColumns<TRow>();
            _rowGroupSize = rowGroupSize;
            _fileWriter = new ParquetFileWriter(path, _columns, compression);
            _accessors = BuildAccessors();
            _buffers = CreateBuffers();
        }

        ColumnAccessor[] BuildAccessors()
        {
            var rowType = typeof(TRow);
            bool isScalar = IsScalarType(rowType);

            if (isScalar)
            {
                return new[] { ColumnAccessor.ForScalar(rowType) };
            }
            else
            {
                var properties = rowType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                var accessors = new ColumnAccessor[properties.Length];
                for (int i = 0; i < properties.Length; i++)
                {
                    accessors[i] = ColumnAccessor.ForProperty(properties[i]);
                }
                return accessors;
            }
        }

        internal void AppendRow(TRow row)
        {
            for (int i = 0; i < _accessors.Length; i++)
            {
                _buffers[i].Add(_accessors[i].GetValue(row!));
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
            for (int i = 0; i < _columns.Length; i++)
            {
                using var colWriter = rowGroupWriter.NextColumn();
                var logicalType = _columns[i].LogicalSystemType;
                WriteColumnDynamic(colWriter, logicalType, _buffers[i]);
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
            var buffers = new System.Collections.IList[_columns.Length];
            for (int i = 0; i < _columns.Length; i++)
            {
                var logicalType = _columns[i].LogicalSystemType;
                var listType = typeof(List<>).MakeGenericType(logicalType);
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

        static bool IsScalarType(Type type)
        {
            return type.IsPrimitive
                || type == typeof(string)
                || type == typeof(byte[])
                || type == typeof(decimal)
                || type == typeof(DateTime)
                || type == typeof(TimeSpan)
                || type == typeof(Guid)
                || IsNullable(type)
                || type.IsEnum
                || (type.IsArray && type != typeof(byte[]))
                || IsGenericList(type);
        }

        static bool IsNullable(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        static bool IsGenericList(Type type)
        {
            if (!type.IsGenericType) return false;
            var def = type.GetGenericTypeDefinition();
            return def == typeof(List<>) || def == typeof(IList<>) || def == typeof(IEnumerable<>);
        }

        /// <summary>
        /// Extracts a value from a row object and converts it to the type expected by the column buffer.
        /// </summary>
        sealed class ColumnAccessor
        {
            readonly Func<object, object?> _getter;

            ColumnAccessor(Func<object, object?> getter)
            {
                _getter = getter;
            }

            public object? GetValue(object row) => _getter(row);

            /// <summary>
            /// Accessor for a scalar row type (the row itself is the value).
            /// </summary>
            public static ColumnAccessor ForScalar(Type rowType)
            {
                if (rowType.IsEnum)
                {
                    // Convert enum → underlying integer type
                    var underlying = Enum.GetUnderlyingType(rowType);
                    return new ColumnAccessor(row => Convert.ChangeType(row, underlying));
                }
                return new ColumnAccessor(row => row);
            }

            /// <summary>
            /// Accessor for a property on the row type.
            /// </summary>
            public static ColumnAccessor ForProperty(PropertyInfo property)
            {
                var propType = property.PropertyType;

                if (propType.IsEnum)
                {
                    var underlying = Enum.GetUnderlyingType(propType);
                    return new ColumnAccessor(row =>
                    {
                        var val = property.GetValue(row);
                        return val == null ? null : Convert.ChangeType(val, underlying);
                    });
                }

                // Nullable enum: Nullable<TEnum> — need to convert to Nullable<TUnderlying>
                if (IsNullable(propType))
                {
                    var inner = Nullable.GetUnderlyingType(propType)!;
                    if (inner.IsEnum)
                    {
                        var underlying = Enum.GetUnderlyingType(inner);
                        return new ColumnAccessor(row =>
                        {
                            var val = property.GetValue(row);
                            if (val == null) return null;
                            return Convert.ChangeType(val, underlying);
                        });
                    }
                }

                return new ColumnAccessor(row => property.GetValue(row));
            }

            static bool IsNullable(Type type)
            {
                return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
            }
        }
    }
}
