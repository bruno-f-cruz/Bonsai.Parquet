using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using global::Apache.Arrow;
using global::Apache.Arrow.Memory;
using global::Apache.Arrow.Types;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Arrow
{
    // Compiles each scalar column once per subscription. The batch loop accesses
    // typed rows directly, without boxing values or allocating object[] columns.
    internal static class ColumnConverter
    {
        internal static Func<IList<TSource>, IArrowArray> Create<TSource>(MemberColumn member)
        {
            var type = Nullable.GetUnderlyingType(member.Type) ?? member.Type;
            if (type.IsEnum) type = Enum.GetUnderlyingType(type);
            if (type == typeof(bool)) return Scalar<TSource, bool, BooleanArray.Builder>(member, () => new BooleanArray.Builder(), (b, v) => b.Append(v));
            if (type == typeof(sbyte)) return Scalar<TSource, sbyte, Int8Array.Builder>(member, () => new Int8Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(byte)) return Scalar<TSource, byte, UInt8Array.Builder>(member, () => new UInt8Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(short)) return Scalar<TSource, short, Int16Array.Builder>(member, () => new Int16Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(ushort)) return Scalar<TSource, ushort, UInt16Array.Builder>(member, () => new UInt16Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(int)) return Scalar<TSource, int, Int32Array.Builder>(member, () => new Int32Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(uint)) return Scalar<TSource, uint, UInt32Array.Builder>(member, () => new UInt32Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(long)) return Scalar<TSource, long, Int64Array.Builder>(member, () => new Int64Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(ulong)) return Scalar<TSource, ulong, UInt64Array.Builder>(member, () => new UInt64Array.Builder(), (b, v) => b.Append(v));
            if (type == typeof(float)) return Scalar<TSource, float, FloatArray.Builder>(member, () => new FloatArray.Builder(), (b, v) => b.Append(v));
            if (type == typeof(double)) return Scalar<TSource, double, DoubleArray.Builder>(member, () => new DoubleArray.Builder(), (b, v) => b.Append(v));
            if (type == typeof(string)) return Scalar<TSource, string, StringArray.Builder>(member, () => new StringArray.Builder(), (b, v) => b.Append(v, null));
            if (type == typeof(byte[])) return Scalar<TSource, byte[], BinaryArray.Builder>(member, () => new BinaryArray.Builder(), (b, v) => AppendBytes(b, v));
            if (type == typeof(Guid)) return Scalar<TSource, Guid, BinaryArray.Builder>(member, () => new BinaryArray.Builder(), (b, v) => AppendGuid(b, v));
            if (type == typeof(decimal)) return Scalar<TSource, decimal, Decimal128Array.Builder>(member,
                () => new Decimal128Array.Builder(new Decimal128Type(28, 12)), (b, v) => b.Append(v));
            if (type == typeof(DateTime)) return Scalar<TSource, DateTime, TimestampArray.Builder>(member,
                () => new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null)),
                (b, v) => b.Append(new DateTimeOffset(DateTime.SpecifyKind(v, DateTimeKind.Utc))));
            if (type == typeof(TimeSpan)) return Scalar<TSource, TimeSpan, DurationArray.Builder>(member,
                () => new DurationArray.Builder(DurationType.Microsecond), (b, v) => b.Append(v));

            // Lists retain the general recursive conversion, including nested lists
            // and nullable elements. Scalar columns use the compiled path above.
            var accessor = member.Accessor;
            return rows =>
            {
                var values = new object?[rows.Count];
                for (int i = 0; i < rows.Count; i++) values[i] = accessor(rows[i]!);
                return SchemaInference.BuildArray(member.Type, values);
            };
        }

        static BinaryArray.Builder AppendBytes(BinaryArray.Builder builder, byte[] value) => builder.Append(new ReadOnlySpan<byte>(value));
        static BinaryArray.Builder AppendGuid(BinaryArray.Builder builder, Guid value) => AppendBytes(builder, value.ToByteArray());

        static Func<IList<TSource>, IArrowArray> Scalar<TSource, TValue, TBuilder>(MemberColumn member,
            Expression<Func<TBuilder>> create, Expression<Func<TBuilder, TValue, TBuilder>> append)
        {
            var rows = Expression.Parameter(typeof(IList<TSource>), "rows");
            var builder = Expression.Variable(typeof(TBuilder), "builder");
            var index = Expression.Variable(typeof(int), "index");
            var count = Expression.Variable(typeof(int), "count");
            var value = Expression.Variable(member.Type, "value");
            var row = Expression.Property(rows, "Item", index);
            var read = member.GetValue(row);
            var nullable = Nullable.GetUnderlyingType(member.Type) != null;
            Expression scalar = nullable ? Expression.Property(value, "Value") : value;
            scalar = TypedExpression.ConvertValue(scalar, typeof(TValue));
            var write = TypedExpression.ReplaceParameter(append.Body, append.Parameters[0], builder);
            write = TypedExpression.ReplaceParameter(write, append.Parameters[1], scalar);
            Expression writeValue = Expression.Block(write, Expression.Empty());
            if (nullable || !member.Type.IsValueType)
            {
                var hasValue = nullable ? (Expression)Expression.Property(value, "HasValue")
                    : Expression.NotEqual(value, Expression.Constant(null, member.Type));
                writeValue = Expression.IfThenElse(hasValue, writeValue,
                    Expression.Block(Expression.Call(builder, "AppendNull", null), Expression.Empty()));
            }
            var done = Expression.Label("done");
            var loop = Expression.Loop(Expression.IfThenElse(Expression.LessThan(index, count),
                Expression.Block(Expression.Assign(value, read), writeValue,
                    Expression.PostIncrementAssign(index), Expression.Empty()), Expression.Break(done)), done);
            var body = Expression.Block(new[] { builder, index, count, value },
                Expression.Assign(builder, create.Body),
                Expression.Assign(count, Expression.Property(Expression.Convert(rows, typeof(ICollection<TSource>)), "Count")),
                Expression.Call(builder, "Reserve", null, count),
                Expression.Assign(index, Expression.Constant(0)), loop,
                Expression.Convert(Expression.Call(builder, "Build", null,
                    Expression.Constant(null, typeof(MemoryAllocator))), typeof(IArrowArray)));
            return Expression.Lambda<Func<IList<TSource>, IArrowArray>>(body, rows).Compile();
        }
    }
}
