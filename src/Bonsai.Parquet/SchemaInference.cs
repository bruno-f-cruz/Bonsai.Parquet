using System;
using System.Collections.Generic;
using System.Reflection;
using ParquetSharp;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Infers a ParquetSharp column array from a C# row type.
    /// </summary>
    internal static class SchemaInference
    {
        const int DecimalPrecision = 28;
        const int DecimalScale = 12;

        /// <summary>
        /// Builds the column descriptors for <typeparamref name="TRow"/>.
        /// If <typeparamref name="TRow"/> is a scalar primitive, a single column named "Value" is returned.
        /// Otherwise one column per public instance property is returned, in declaration order.
        /// </summary>
        internal static Column[] InferColumns<TRow>()
        {
            return InferColumns(typeof(TRow));
        }

        internal static Column[] InferColumns(Type rowType)
        {
            if (IsTier1(rowType) || IsNullable(rowType) || IsArray(rowType) || IsEnum(rowType))
            {
                return new[] { MakeColumn("Value", rowType) };
            }

            var properties = rowType.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            if (properties.Length == 0)
            {
                throw new NotSupportedException(
                    $"Type '{rowType.FullName}' has no public instance properties and is not a supported scalar type. " +
                    "Flatten nested objects via the Selector expression before writing.");
            }

            var columns = new Column[properties.Length];
            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                try
                {
                    columns[i] = MakeColumn(prop.Name, prop.PropertyType);
                }
                catch (NotSupportedException ex)
                {
                    throw new NotSupportedException(
                        $"Property '{prop.Name}' on type '{rowType.FullName}' has unsupported type '{prop.PropertyType.FullName}'. " +
                        ex.Message, ex);
                }
            }
            return columns;
        }

        static Column MakeColumn(string name, Type type)
        {
            if (IsNullable(type))
            {
                var inner = Nullable.GetUnderlyingType(type)!;
                return MakeNullableColumn(name, inner);
            }

            if (IsArray(type))
            {
                return MakeArrayColumn(name, type);
            }

            if (IsEnum(type))
            {
                var underlying = Enum.GetUnderlyingType(type);
                return MakeScalarColumn(name, underlying);
            }

            return MakeScalarColumn(name, type);
        }

        static Column MakeScalarColumn(string name, Type type)
        {
            if (type == typeof(bool)) return new Column<bool>(name);
            if (type == typeof(sbyte)) return new Column(typeof(sbyte), name, LogicalType.Int(8, true));
            if (type == typeof(byte)) return new Column(typeof(byte), name, LogicalType.Int(8, false));
            if (type == typeof(short)) return new Column(typeof(short), name, LogicalType.Int(16, true));
            if (type == typeof(ushort)) return new Column(typeof(ushort), name, LogicalType.Int(16, false));
            if (type == typeof(int)) return new Column<int>(name);
            if (type == typeof(uint)) return new Column(typeof(uint), name, LogicalType.Int(32, false));
            if (type == typeof(long)) return new Column<long>(name);
            if (type == typeof(ulong)) return new Column(typeof(ulong), name, LogicalType.Int(64, false));
            if (type == typeof(float)) return new Column<float>(name);
            if (type == typeof(double)) return new Column<double>(name);
            // Decimal: defaults to precision 28, scale 12. DateTime semantics: local-time (isAdjustedToUTC=false).
            if (type == typeof(decimal)) return new Column(typeof(decimal), name, LogicalType.Decimal(DecimalPrecision, DecimalScale));
            if (type == typeof(string)) return new Column<string>(name);
            if (type == typeof(byte[])) return new Column<byte[]>(name);
            // DateTime: stored as TIMESTAMP(MICROS, isAdjustedToUTC=false); local-time semantics.
            if (type == typeof(DateTime)) return new Column(typeof(DateTime), name, LogicalType.Timestamp(false, TimeUnit.Micros));
            if (type == typeof(TimeSpan)) return new Column(typeof(TimeSpan), name, LogicalType.Time(false, TimeUnit.Micros));
            if (type == typeof(Guid)) return new Column<Guid>(name);

            throw new NotSupportedException(
                $"Type '{type.FullName}' is not a supported Tier-1 parquet type. " +
                "Supported types: bool, sbyte, byte, short, ushort, int, uint, long, ulong, float, double, decimal, string, byte[], DateTime, TimeSpan, Guid.");
        }

        static Column MakeNullableColumn(string name, Type inner)
        {
            var nullableType = typeof(Nullable<>).MakeGenericType(inner);

            if (inner == typeof(bool)) return new Column<bool?>(name);
            if (inner == typeof(sbyte)) return new Column(nullableType, name, LogicalType.Int(8, true));
            if (inner == typeof(byte)) return new Column(nullableType, name, LogicalType.Int(8, false));
            if (inner == typeof(short)) return new Column(nullableType, name, LogicalType.Int(16, true));
            if (inner == typeof(ushort)) return new Column(nullableType, name, LogicalType.Int(16, false));
            if (inner == typeof(int)) return new Column<int?>(name);
            if (inner == typeof(uint)) return new Column(nullableType, name, LogicalType.Int(32, false));
            if (inner == typeof(long)) return new Column<long?>(name);
            if (inner == typeof(ulong)) return new Column(nullableType, name, LogicalType.Int(64, false));
            if (inner == typeof(float)) return new Column<float?>(name);
            if (inner == typeof(double)) return new Column<double?>(name);
            if (inner == typeof(decimal)) return new Column(nullableType, name, LogicalType.Decimal(DecimalPrecision, DecimalScale));
            if (inner == typeof(DateTime)) return new Column(nullableType, name, LogicalType.Timestamp(false, TimeUnit.Micros));
            if (inner == typeof(TimeSpan)) return new Column(nullableType, name, LogicalType.Time(false, TimeUnit.Micros));
            if (inner == typeof(Guid)) return new Column<Guid?>(name);

            if (inner.IsEnum)
            {
                var underlying = Enum.GetUnderlyingType(inner);
                var nullableUnderlying = typeof(Nullable<>).MakeGenericType(underlying);
                return MakeNullableColumn(name, underlying);
            }

            throw new NotSupportedException(
                $"Nullable<{inner.FullName}> is not supported. Only Nullable<T> of Tier-1 value types is supported.");
        }

        static Column MakeArrayColumn(string name, Type type)
        {
            Type elementType;
            if (type.IsArray)
            {
                elementType = type.GetElementType()!;
            }
            else if (IsGenericList(type, out var listElement))
            {
                elementType = listElement!;
            }
            else
            {
                throw new NotSupportedException($"Type '{type.FullName}' is not a supported array/list type.");
            }

            // Map element type to its array equivalent for the Column descriptor
            if (elementType == typeof(bool)) return new Column<bool[]>(name);
            if (elementType == typeof(sbyte)) return new Column(typeof(sbyte[]), name, LogicalType.Int(8, true));
            if (elementType == typeof(byte)) return new Column<byte[]>(name); // byte[] is BYTE_ARRAY, not LIST
            if (elementType == typeof(short)) return new Column(typeof(short[]), name, LogicalType.Int(16, true));
            if (elementType == typeof(ushort)) return new Column(typeof(ushort[]), name, LogicalType.Int(16, false));
            if (elementType == typeof(int)) return new Column<int[]>(name);
            if (elementType == typeof(uint)) return new Column(typeof(uint[]), name, LogicalType.Int(32, false));
            if (elementType == typeof(long)) return new Column<long[]>(name);
            if (elementType == typeof(ulong)) return new Column(typeof(ulong[]), name, LogicalType.Int(64, false));
            if (elementType == typeof(float)) return new Column<float[]>(name);
            if (elementType == typeof(double)) return new Column<double[]>(name);
            if (elementType == typeof(string)) return new Column<string[]>(name);
            if (elementType == typeof(DateTime)) return new Column(typeof(DateTime[]), name, LogicalType.Timestamp(false, TimeUnit.Micros));
            if (elementType == typeof(TimeSpan)) return new Column(typeof(TimeSpan[]), name, LogicalType.Time(false, TimeUnit.Micros));
            if (elementType == typeof(Guid)) return new Column<Guid[]>(name);

            throw new NotSupportedException(
                $"Array element type '{elementType.FullName}' is not supported. Only Tier-1 element types are supported in arrays.");
        }

        static bool IsTier1(Type type)
        {
            return type == typeof(bool)
                || type == typeof(sbyte) || type == typeof(byte)
                || type == typeof(short) || type == typeof(ushort)
                || type == typeof(int) || type == typeof(uint)
                || type == typeof(long) || type == typeof(ulong)
                || type == typeof(float) || type == typeof(double)
                || type == typeof(decimal)
                || type == typeof(string)
                || type == typeof(byte[])
                || type == typeof(DateTime)
                || type == typeof(TimeSpan)
                || type == typeof(Guid);
        }

        static bool IsNullable(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        static bool IsArray(Type type)
        {
            if (type == typeof(byte[])) return false; // byte[] is raw bytes, not LIST
            if (type.IsArray) return true;
            return IsGenericList(type, out _);
        }

        static bool IsEnum(Type type)
        {
            return type.IsEnum;
        }

        static bool IsGenericList(Type type, out Type? elementType)
        {
            if (type.IsGenericType)
            {
                var def = type.GetGenericTypeDefinition();
                if (def == typeof(List<>) || def == typeof(IList<>) || def == typeof(IEnumerable<>))
                {
                    elementType = type.GetGenericArguments()[0];
                    return true;
                }
            }
            elementType = null;
            return false;
        }
    }
}
