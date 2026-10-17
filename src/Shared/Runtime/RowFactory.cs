using System;
using System.Linq;
using System.Linq.Expressions;

namespace Bonsai.Apache.Common
{
    internal static class RowFactory
    {
        static readonly Type[] TupleTypes = { typeof(Tuple<>), typeof(Tuple<,>), typeof(Tuple<,,>),
            typeof(Tuple<,,,>), typeof(Tuple<,,,,>), typeof(Tuple<,,,,,>), typeof(Tuple<,,,,,,>), typeof(Tuple<,,,,,,,>) };

        internal static Type GetOutputType(Type[] types) => types.Length == 1 ? types[0] : GetTupleType(types);

        static Type GetTupleType(Type[] types)
        {
            var arguments = types.Length <= 7 ? types : types.Take(7).Concat(new[] { GetTupleType(types.Skip(7).ToArray()) }).ToArray();
            return TupleTypes[arguments.Length - 1].MakeGenericType(arguments);
        }

        internal static Func<Array[], int, T> Create<T>(Type[] types)
        {
            var buffers = Expression.Parameter(typeof(Array[]), "buffers");
            var index = Expression.Parameter(typeof(int), "index");
            var values = types.Select((type, i) => (Expression)Expression.ArrayIndex(
                Expression.Convert(Expression.ArrayIndex(buffers, Expression.Constant(i)), type.MakeArrayType()), index)).ToArray();
            var body = values.Length == 1 ? values[0] : CreateTuple(values);
            return Expression.Lambda<Func<Array[], int, T>>(Expression.Convert(body, typeof(T)), buffers, index).Compile();
        }

        static Expression CreateTuple(Expression[] values)
        {
            var arguments = values.Length <= 7 ? values : values.Take(7).Concat(new[] { CreateTuple(values.Skip(7).ToArray()) }).ToArray();
            var type = TupleTypes[arguments.Length - 1].MakeGenericType(arguments.Select(a => a.Type).ToArray());
            return Expression.New(type.GetConstructors()[0], arguments);
        }
    }
}
