using System;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Bonsai.Apache.Common;
using ParquetSharp;

namespace Bonsai.Apache.Parquet
{
    internal abstract class ColumnBuffer<TRow>
    {
        internal abstract void Append(TRow row, int index);
        internal abstract void Write(ColumnWriter writer, int count);
        internal abstract void Clear(int index, int count);

        internal static ColumnBuffer<TRow> Create(ColumnPlan plan, int capacity)
        {
            // Resolve the logical element type once, before opening the output file.
            var type = typeof(TypedColumnBuffer<,>).MakeGenericType(typeof(TRow), plan.LogicalSystemType);
            try
            {
                return (ColumnBuffer<TRow>)Activator.CreateInstance(type,
                    BindingFlags.Instance | BindingFlags.NonPublic, null, new object[] { plan.Member, capacity }, null)!;
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                throw;
            }
        }
    }

    internal sealed class TypedColumnBuffer<TRow, TValue> : ColumnBuffer<TRow>
    {
        readonly Func<TRow, TValue> accessor;
        readonly int capacity;
        TValue[] values;

        internal TypedColumnBuffer(MemberColumn member, int capacity)
        {
            accessor = member.CompileAccessor<TRow, TValue>();
            this.capacity = capacity;
            // Grow up to the row group size, avoiding large allocations for short
            // recordings while retaining the buffer for subsequent row groups.
            values = new TValue[Math.Min(capacity, 1024)];
        }

        internal override void Append(TRow row, int index)
        {
            var value = accessor(row);
            if (index == values.Length)
                Array.Resize(ref values, values.Length + Math.Min(values.Length, capacity - values.Length));
            values[index] = value;
        }

        internal override void Write(ColumnWriter column, int count)
        {
            using var writer = column.LogicalWriter<TValue>();
            writer.WriteBatch(values, 0, count);
        }

        internal override void Clear(int index, int count)
        {
            // Supported logical value types contain no managed references.
            if (!typeof(TValue).IsValueType && index < values.Length)
                Array.Clear(values, index, Math.Min(count, values.Length - index));
        }
    }
}
