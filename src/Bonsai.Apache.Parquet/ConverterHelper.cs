using System;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    /// <summary>
    /// Provides helper methods for converting between Parquet column types,
    /// <see cref="ParquetSharp"/> schema descriptors, and CLR types used by
    /// the Bonsai.Apache.Parquet operators.
    /// </summary>
    public static class ConverterHelper
    {
        /// <summary>
        /// Determines the <see cref="ParquetColumnType"/> that best matches the
        /// element type declared by the specified <see cref="ColumnDescriptor"/>.
        /// </summary>
        /// <param name="col">The Parquet column descriptor to inspect.</param>
        /// <returns>
        /// The <see cref="ParquetColumnType"/> corresponding to the column's
        /// element type. <see cref="Nullable{T}"/> wrappers and array element
        /// types (other than <see cref="T:System.Byte[]"/>) are unwrapped before
        /// mapping. When the element type cannot be resolved, a fallback based
        /// on the column's physical type is returned.
        /// </returns>
        internal static ParquetColumnType ToParquetType(ColumnDescriptor col)
        {
            var (_, _, elementType) = col.GetSystemTypes(LogicalTypeFactory.Default, null);
            var baseType = elementType;
            if (baseType != null && baseType.IsGenericType &&
                baseType.GetGenericTypeDefinition() == typeof(Nullable<>))
                baseType = baseType.GetGenericArguments()[0];
            if (baseType != null && baseType.IsArray && baseType != typeof(byte[]))
                baseType = baseType.GetElementType();

            if (baseType == typeof(bool))     return ParquetColumnType.Bool;
            if (baseType == typeof(sbyte))    return ParquetColumnType.SByte;
            if (baseType == typeof(byte))     return ParquetColumnType.Byte;
            if (baseType == typeof(short))    return ParquetColumnType.Int16;
            if (baseType == typeof(ushort))   return ParquetColumnType.UInt16;
            if (baseType == typeof(int))      return ParquetColumnType.Int32;
            if (baseType == typeof(uint))     return ParquetColumnType.UInt32;
            if (baseType == typeof(long))     return ParquetColumnType.Int64;
            if (baseType == typeof(ulong))    return ParquetColumnType.UInt64;
            if (baseType == typeof(float))    return ParquetColumnType.Float;
            if (baseType == typeof(double))   return ParquetColumnType.Double;
            if (baseType == typeof(decimal))  return ParquetColumnType.Decimal;
            if (baseType == typeof(string))   return ParquetColumnType.String;
            if (baseType == typeof(byte[]))   return ParquetColumnType.Bytes;
            if (baseType == typeof(DateTime)) return ParquetColumnType.DateTime;
            if (baseType == typeof(TimeSpan)) return ParquetColumnType.TimeSpan;
            if (baseType == typeof(Guid))     return ParquetColumnType.Guid;

            return col.PhysicalType switch
            {
                PhysicalType.Boolean           => ParquetColumnType.Bool,
                PhysicalType.Float             => ParquetColumnType.Float,
                PhysicalType.Double            => ParquetColumnType.Double,
                PhysicalType.Int32             => ParquetColumnType.Int32,
                PhysicalType.Int64             => ParquetColumnType.Int64,
                PhysicalType.ByteArray         => ParquetColumnType.Bytes,
                PhysicalType.FixedLenByteArray => ParquetColumnType.Bytes,
                _                              => ParquetColumnType.String,
            };
        }

        /// <summary>
        /// Infers a set of <see cref="ColumnDefinition"/> entries from a
        /// Parquet <see cref="SchemaDescriptor"/>, capturing each column's
        /// name, type, nullability, and whether it represents a list.
        /// </summary>
        /// <param name="schema">The Parquet schema to inspect.</param>
        /// <returns>
        /// An array of <see cref="ColumnDefinition"/> values describing every
        /// column in <paramref name="schema"/>, in declaration order.
        /// </returns>
        public static ColumnDefinition[] InferColumns(SchemaDescriptor schema)
        {
            var result = new ColumnDefinition[schema.NumColumns];
            for (int i = 0; i < schema.NumColumns; i++)
            {
                var col = schema.Column(i);
                var (_, _, elementType) = col.GetSystemTypes(LogicalTypeFactory.Default, null);
                var t = elementType;
                if (t?.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(Nullable<>))
                    t = t.GetGenericArguments()[0];
                bool isArray = t?.IsArray == true && t != typeof(byte[]);

                result[i] = new ColumnDefinition
                {
                    Name = schema.ColumnRoot(i).Name,
                    Type = ToParquetType(col),
                    IsNullable = col.MaxDefinitionLevel > 0,
                    IsArray = isArray,
                };
            }
            return result;
        }
    }
}
