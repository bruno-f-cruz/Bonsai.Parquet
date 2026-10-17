using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using global::Apache.Arrow;
using global::Apache.Arrow.Types;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Arrow
{
    internal static class SchemaInference
    {
        internal static IArrowType GetArrowType(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);
            var element = MemberSchema.GetElementType(type);
            if (element != null) return new ListType(GetArrowType(element));
            if (type == typeof(bool)) return BooleanType.Default;
            if (type == typeof(sbyte)) return Int8Type.Default;
            if (type == typeof(byte)) return UInt8Type.Default;
            if (type == typeof(short)) return Int16Type.Default;
            if (type == typeof(ushort)) return UInt16Type.Default;
            if (type == typeof(int)) return Int32Type.Default;
            if (type == typeof(uint)) return UInt32Type.Default;
            if (type == typeof(long)) return Int64Type.Default;
            if (type == typeof(ulong)) return UInt64Type.Default;
            if (type == typeof(float)) return FloatType.Default;
            if (type == typeof(double)) return DoubleType.Default;
            if (type == typeof(decimal)) return new Decimal128Type(28, 12);
            if (type == typeof(string)) return StringType.Default;
            if (type == typeof(byte[]) || type == typeof(Guid)) return BinaryType.Default;
            if (type == typeof(DateTime)) return new TimestampType(TimeUnit.Microsecond, (string?)null);
            if (type == typeof(TimeSpan)) return DurationType.Microsecond;
            throw new NotSupportedException($"Type '{type.FullName}' is not a supported Arrow column type. Select scalar members with Selector.");
        }

        internal static Schema GetSchema(MemberColumn[] members) => new Schema(members.Select(m =>
            new Field(m.Name, GetArrowType(m.Type), !m.Type.IsValueType || Nullable.GetUnderlyingType(m.Type) != null)), null);

        internal static IArrowArray BuildArray(Type type, object?[] values)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum)
            {
                type = Enum.GetUnderlyingType(type);
                values = values.Select(v => v == null ? null : Convert.ChangeType(v, type)).ToArray();
            }
            var element = MemberSchema.GetElementType(type);
            if (element != null)
            {
                var flattened = new List<object?>();
                var offsets = new ArrowBuffer.Builder<int>();
                var validity = new ArrowBuffer.BitmapBuilder();
                offsets.Reserve(checked(values.Length + 1));
                validity.Reserve(values.Length);
                int nulls = 0;
                foreach (var value in values)
                {
                    offsets.Append(flattened.Count);
                    validity.Append(value != null);
                    if (value == null) nulls++;
                    else foreach (var item in (IEnumerable)value) flattened.Add(item);
                }
                offsets.Append(flattened.Count);
                var child = BuildArray(element, flattened.ToArray());
                return new ListArray(GetArrowType(type), values.Length, offsets.Build(), child, validity.Build(), nulls);
            }
            if (type == typeof(bool)) return Build(values, new BooleanArray.Builder());
            if (type == typeof(sbyte)) return Build(values, new Int8Array.Builder());
            if (type == typeof(byte)) return Build(values, new UInt8Array.Builder());
            if (type == typeof(short)) return Build(values, new Int16Array.Builder());
            if (type == typeof(ushort)) return Build(values, new UInt16Array.Builder());
            if (type == typeof(int)) return Build(values, new Int32Array.Builder());
            if (type == typeof(uint)) return Build(values, new UInt32Array.Builder());
            if (type == typeof(long)) return Build(values, new Int64Array.Builder());
            if (type == typeof(ulong)) return Build(values, new UInt64Array.Builder());
            if (type == typeof(float)) return Build(values, new FloatArray.Builder());
            if (type == typeof(double)) return Build(values, new DoubleArray.Builder());
            if (type == typeof(string))
            {
                var builder = new StringArray.Builder();
                builder.Reserve(values.Length);
                foreach (var value in values) { if (value == null) builder.AppendNull(); else builder.Append((string)value); }
                return builder.Build();
            }
            if (type == typeof(byte[]) || type == typeof(Guid))
            {
                var builder = new BinaryArray.Builder();
                builder.Reserve(values.Length);
                foreach (var value in values)
                {
                    if (value == null) builder.AppendNull();
                    else builder.Append(new ReadOnlySpan<byte>(type == typeof(Guid) ? ((Guid)value).ToByteArray() : (byte[])value));
                }
                return builder.Build();
            }
            if (type == typeof(decimal))
            {
                var builder = new Decimal128Array.Builder(new Decimal128Type(28, 12));
                builder.Reserve(values.Length);
                foreach (var value in values) { if (value == null) builder.AppendNull(); else builder.Append((decimal)value); }
                return builder.Build();
            }
            if (type == typeof(DateTime))
            {
                // Match Parquet's local timestamp semantics: preserve wall-clock ticks without time zone conversion.
                var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null));
                builder.Reserve(values.Length);
                foreach (var value in values)
                {
                    if (value == null) builder.AppendNull();
                    else builder.Append(new DateTimeOffset(DateTime.SpecifyKind((DateTime)value, DateTimeKind.Utc)));
                }
                return builder.Build();
            }
            if (type == typeof(TimeSpan))
            {
                var builder = new DurationArray.Builder(DurationType.Microsecond);
                builder.Reserve(values.Length);
                foreach (var value in values) { if (value == null) builder.AppendNull(); else builder.Append((TimeSpan)value); }
                return builder.Build();
            }
            throw new NotSupportedException($"Unsupported Arrow column type '{type}'.");
        }

        static IArrowArray Build<T, TArray, TBuilder>(object?[] values, IArrowArrayBuilder<T, TArray, TBuilder> builder)
            where TArray : IArrowArray
            where TBuilder : IArrowArrayBuilder<T, TArray, TBuilder>
        {
            builder.Reserve(values.Length);
            foreach (var value in values)
            {
                if (value == null) builder.AppendNull();
                else builder.Append((T)value);
            }
            return builder.Build(null);
        }
    }
}
