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
            return type switch
            {
                Type t when t == typeof(bool) => new Column<bool>(name),
                Type t when t == typeof(sbyte) => new Column(typeof(sbyte), name, LogicalType.Int(8, true)),
                Type t when t == typeof(byte) => new Column(typeof(byte), name, LogicalType.Int(8, false)),
                Type t when t == typeof(short) => new Column(typeof(short), name, LogicalType.Int(16, true)),
                Type t when t == typeof(ushort) => new Column(typeof(ushort), name, LogicalType.Int(16, false)),
                Type t when t == typeof(int) => new Column<int>(name),
                Type t when t == typeof(uint) => new Column(typeof(uint), name, LogicalType.Int(32, false)),
                Type t when t == typeof(long) => new Column<long>(name),
                Type t when t == typeof(ulong) => new Column(typeof(ulong), name, LogicalType.Int(64, false)),
                Type t when t == typeof(float) => new Column<float>(name),
                Type t when t == typeof(double) => new Column<double>(name),
                // Decimal: defaults to precision 28, scale 12.
                Type t when t == typeof(decimal) => new Column(typeof(decimal), name, LogicalType.Decimal(DecimalPrecision, DecimalScale)),
                Type t when t == typeof(string) => new Column<string>(name),
                Type t when t == typeof(byte[]) => new Column<byte[]>(name),
                // DateTime: stored as TIMESTAMP(MICROS, isAdjustedToUTC=false); local-time semantics.
                Type t when t == typeof(DateTime) => new Column(typeof(DateTime), name, LogicalType.Timestamp(false, TimeUnit.Micros)),
                Type t when t == typeof(TimeSpan) => new Column(typeof(TimeSpan), name, LogicalType.Time(false, TimeUnit.Micros)),
                Type t when t == typeof(Guid) => new Column<Guid>(name),
                _ => throw new NotSupportedException(
                    $"Type '{type.FullName}' is not a supported Tier-1 parquet type. " +
                    "Supported types: bool, sbyte, byte, short, ushort, int, uint, long, ulong, float, double, decimal, string, byte[], DateTime, TimeSpan, Guid.")
            };
        }

        static Column MakeNullableColumn(string name, Type inner)
        {
            var nullableType = typeof(Nullable<>).MakeGenericType(inner);

            return inner switch
            {
                Type t when t == typeof(bool) => new Column<bool?>(name),
                Type t when t == typeof(sbyte) => new Column(nullableType, name, LogicalType.Int(8, true)),
                Type t when t == typeof(byte) => new Column(nullableType, name, LogicalType.Int(8, false)),
                Type t when t == typeof(short) => new Column(nullableType, name, LogicalType.Int(16, true)),
                Type t when t == typeof(ushort) => new Column(nullableType, name, LogicalType.Int(16, false)),
                Type t when t == typeof(int) => new Column<int?>(name),
                Type t when t == typeof(uint) => new Column(nullableType, name, LogicalType.Int(32, false)),
                Type t when t == typeof(long) => new Column<long?>(name),
                Type t when t == typeof(ulong) => new Column(nullableType, name, LogicalType.Int(64, false)),
                Type t when t == typeof(float) => new Column<float?>(name),
                Type t when t == typeof(double) => new Column<double?>(name),
                Type t when t == typeof(decimal) => new Column(nullableType, name, LogicalType.Decimal(DecimalPrecision, DecimalScale)),
                Type t when t == typeof(DateTime) => new Column(nullableType, name, LogicalType.Timestamp(false, TimeUnit.Micros)),
                Type t when t == typeof(TimeSpan) => new Column(nullableType, name, LogicalType.Time(false, TimeUnit.Micros)),
                Type t when t == typeof(Guid) => new Column<Guid?>(name),
                Type t when t.IsEnum => MakeNullableColumn(name, Enum.GetUnderlyingType(t)),
                _ => throw new NotSupportedException(
                    $"Nullable<{inner.FullName}> is not supported. Only Nullable<T> of Tier-1 value types is supported.")
            };
        }

        static Column MakeArrayColumn(string name, Type type)
        {
            Type elementType = type switch
            {
                Type t when t.IsArray => t.GetElementType()!,
                Type t when IsGenericList(t, out var listElement) => listElement!,
                _ => throw new NotSupportedException($"Type '{type.FullName}' is not a supported array/list type.")
            };

            // Map element type to its array equivalent for the Column descriptor
            return elementType switch
            {
                Type t when t == typeof(bool) => new Column<bool[]>(name),
                Type t when t == typeof(sbyte) => new Column(typeof(sbyte[]), name, LogicalType.Int(8, true)),
                Type t when t == typeof(byte) => new Column<byte[]>(name), // byte[] is BYTE_ARRAY, not LIST
                Type t when t == typeof(short) => new Column(typeof(short[]), name, LogicalType.Int(16, true)),
                Type t when t == typeof(ushort) => new Column(typeof(ushort[]), name, LogicalType.Int(16, false)),
                Type t when t == typeof(int) => new Column<int[]>(name),
                Type t when t == typeof(uint) => new Column(typeof(uint[]), name, LogicalType.Int(32, false)),
                Type t when t == typeof(long) => new Column<long[]>(name),
                Type t when t == typeof(ulong) => new Column(typeof(ulong[]), name, LogicalType.Int(64, false)),
                Type t when t == typeof(float) => new Column<float[]>(name),
                Type t when t == typeof(double) => new Column<double[]>(name),
                Type t when t == typeof(string) => new Column<string[]>(name),
                Type t when t == typeof(DateTime) => new Column(typeof(DateTime[]), name, LogicalType.Timestamp(false, TimeUnit.Micros)),
                Type t when t == typeof(TimeSpan) => new Column(typeof(TimeSpan[]), name, LogicalType.Time(false, TimeUnit.Micros)),
                Type t when t == typeof(Guid) => new Column<Guid[]>(name),
                _ => throw new NotSupportedException(
                    $"Array element type '{elementType.FullName}' is not supported. Only Tier-1 element types are supported in arrays.")
            };
        }

        static bool IsTier1(Type type)
        {
            return type switch
            {
                Type t when t == typeof(bool) => true,
                Type t when t == typeof(sbyte) || t == typeof(byte) => true,
                Type t when t == typeof(short) || t == typeof(ushort) => true,
                Type t when t == typeof(int) || t == typeof(uint) => true,
                Type t when t == typeof(long) || t == typeof(ulong) => true,
                Type t when t == typeof(float) || t == typeof(double) => true,
                Type t when t == typeof(decimal) => true,
                Type t when t == typeof(string) => true,
                Type t when t == typeof(byte[]) => true,
                Type t when t == typeof(DateTime) => true,
                Type t when t == typeof(TimeSpan) => true,
                Type t when t == typeof(Guid) => true,
                _ => false
            };
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
