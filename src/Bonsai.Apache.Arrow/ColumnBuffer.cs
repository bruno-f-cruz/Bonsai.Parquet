using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using global::Apache.Arrow;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Arrow
{
    internal abstract class ColumnBuffer<TRow>
    {
        internal abstract void Append(TRow row);
        internal abstract IArrowArray Build();
        internal abstract void Truncate(int count);

        internal static ColumnBuffer<TRow> Create(MemberColumn member, int capacity)
        {
            var type = member.Type;
            var nullable = Nullable.GetUnderlyingType(type);
            if ((nullable ?? type).IsEnum)
            {
                type = Enum.GetUnderlyingType(nullable ?? type);
                if (nullable != null) type = typeof(Nullable<>).MakeGenericType(type);
            }
            else if (MemberSchema.GetElementType(type) is Type element) type = element.MakeArrayType();
            try
            {
                return (ColumnBuffer<TRow>)Activator.CreateInstance(typeof(TypedColumnBuffer<,>).MakeGenericType(typeof(TRow), type),
                    BindingFlags.Instance | BindingFlags.NonPublic, null, new object[] { member, capacity }, null)!;
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
        readonly Func<IList<TValue>, IArrowArray> convert;
        readonly List<TValue> values;

        internal TypedColumnBuffer(MemberColumn member, int capacity)
        {
            accessor = member.CompileAccessor<TRow, TValue>();
            var value = Expression.Parameter(typeof(TValue), "value");
            convert = ColumnConverter.Create<TValue>(new MemberColumn(member.Name, value, value));
            values = new List<TValue>(Math.Min(capacity, 1024));
        }

        internal override void Append(TRow row) => values.Add(accessor(row));
        internal override IArrowArray Build() => convert(values);
        internal override void Truncate(int count)
        {
            if (values.Count > count) values.RemoveRange(count, values.Count - count);
        }
    }
}
