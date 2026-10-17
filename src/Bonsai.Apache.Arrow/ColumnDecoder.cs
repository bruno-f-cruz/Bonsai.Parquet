using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using global::Apache.Arrow;
using Array = System.Array;

namespace Bonsai.Apache.Arrow
{
    internal abstract class ColumnDecoder
    {
        internal abstract void Read(IArrowArray array, int offset, int count, CancellationToken cancellation);

        internal static ColumnDecoder Open(Type type, Array buffer) => (ColumnDecoder)Activator.CreateInstance(
            typeof(TypedDecoder<>).MakeGenericType(type), buffer, CreateGetter(type))!;

        static Delegate CreateGetter(Type type)
        {
            var array = Expression.Parameter(typeof(IArrowArray), "array");
            var index = Expression.Parameter(typeof(int), "index");
            var scalar = Nullable.GetUnderlyingType(type) ?? type;
            LambdaExpression getter;
            if (scalar.IsArray && scalar != typeof(byte[]))
            {
                var element = scalar.GetElementType()!;
                var list = Expression.Parameter(typeof(ListArray), "list");
                var read = CreateGetter(element);
                var method = typeof(ColumnDecoder).GetMethod(nameof(ReadList), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(element);
                getter = Expression.Lambda(Expression.Call(method, list, index, Expression.Constant(read)), list, index);
            }
            else getter = ScalarGetter(scalar);
            var value = Expression.Convert(Expression.Invoke(getter, Expression.Convert(array, getter.Parameters[0].Type), index), type);
            Expression missing = !type.IsValueType || Nullable.GetUnderlyingType(type) != null
                ? Expression.Default(type)
                : Expression.Throw(Expression.New(typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!,
                    Expression.Constant("A null value cannot be read into a non-nullable value type.")), type);
            var body = Expression.Condition(Expression.Call(array, nameof(IArrowArray.IsNull), null, index), missing, value);
            return Expression.Lambda(typeof(Func<,,>).MakeGenericType(typeof(IArrowArray), typeof(int), type), body, array, index).Compile();
        }

        static LambdaExpression Getter<TArray, TValue>(Expression<Func<TArray, int, TValue>> getter) => getter;

        static LambdaExpression ScalarGetter(Type type)
        {
            if (type == typeof(bool)) return Getter((BooleanArray a, int i) => a.GetValue(i));
            if (type == typeof(sbyte)) return Getter((Int8Array a, int i) => a.GetValue(i));
            if (type == typeof(byte)) return Getter((UInt8Array a, int i) => a.GetValue(i));
            if (type == typeof(short)) return Getter((Int16Array a, int i) => a.GetValue(i));
            if (type == typeof(ushort)) return Getter((UInt16Array a, int i) => a.GetValue(i));
            if (type == typeof(int)) return Getter((Int32Array a, int i) => a.GetValue(i));
            if (type == typeof(uint)) return Getter((UInt32Array a, int i) => a.GetValue(i));
            if (type == typeof(long)) return Getter((Int64Array a, int i) => a.GetValue(i));
            if (type == typeof(ulong)) return Getter((UInt64Array a, int i) => a.GetValue(i));
            if (type == typeof(float)) return Getter((FloatArray a, int i) => a.GetValue(i));
            if (type == typeof(double)) return Getter((DoubleArray a, int i) => a.GetValue(i));
            if (type == typeof(decimal)) return Getter((Decimal128Array a, int i) => a.GetValue(i));
            if (type == typeof(string)) return Getter((StringArray a, int i) => a.GetString(i, null));
            if (type == typeof(byte[])) return Getter((BinaryArray a, int i) => a.GetBytes(i).ToArray());
            if (type == typeof(Guid)) return Getter((BinaryArray a, int i) => new Guid(a.GetBytes(i).ToArray()));
            if (type == typeof(DateTime)) return Getter((TimestampArray a, int i) => a.GetTimestamp(i)!.Value.DateTime);
            if (type == typeof(TimeSpan)) return Getter((DurationArray a, int i) => a.GetTimeSpan(i));
            throw new NotSupportedException($"Unsupported column type '{type}'.");
        }

        static T[] ReadList<T>(ListArray array, int index, Func<IArrowArray, int, T> read)
        {
            var values = new T[array.GetValueLength(index)];
            var offset = array.ValueOffsets[index];
            for (int i = 0; i < values.Length; i++) values[i] = read(array.Values, offset + i);
            return values;
        }

        sealed class TypedDecoder<T> : ColumnDecoder
        {
            readonly T[] buffer;
            readonly Func<IArrowArray, int, T> read;

            public TypedDecoder(Array buffer, Delegate read)
            {
                this.buffer = (T[])buffer;
                this.read = (Func<IArrowArray, int, T>)read;
            }

            internal override void Read(IArrowArray array, int offset, int count, CancellationToken cancellation)
            {
                for (int i = 0; i < count; i++)
                {
                    cancellation.ThrowIfCancellationRequested();
                    buffer[i] = read(array, offset + i);
                }
            }
        }
    }
}
