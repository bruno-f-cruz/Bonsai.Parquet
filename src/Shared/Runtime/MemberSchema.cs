using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Bonsai.Apache.Common
{
    // Linked into each format assembly from the shared source folder.
    internal sealed class MemberColumn
    {
        internal string Name { get; }
        internal Type Type { get; }
        internal LambdaExpression Getter { get; }
        readonly Lazy<Func<object, object?>> accessor;
        internal Func<object, object?> Accessor => accessor.Value;

        internal Expression GetValue(Expression row) => TypedExpression.ReplaceParameter(Getter.Body, Getter.Parameters[0], row);

        internal Func<TRow, TValue> CompileAccessor<TRow, TValue>()
        {
            var row = Expression.Parameter(typeof(TRow), "row");
            var value = Expression.Variable(Type, "value");
            var body = Expression.Block(new[] { value }, Expression.Assign(value, GetValue(row)),
                TypedExpression.ConvertValue(value, typeof(TValue)));
            return Expression.Lambda<Func<TRow, TValue>>(body, row).Compile();
        }

        internal MemberColumn(string name, Expression access, ParameterExpression parameter)
        {
            Name = name;
            Type = access.Type;
            Getter = Expression.Lambda(access, parameter);
            accessor = new Lazy<Func<object, object?>>(() =>
            {
                var input = Expression.Parameter(typeof(object), "input");
                Expression value = GetValue(Expression.Convert(input, parameter.Type));
                var underlying = Nullable.GetUnderlyingType(Type) ?? Type;
                if (underlying.IsEnum)
                {
                    var target = Enum.GetUnderlyingType(underlying);
                    if (Nullable.GetUnderlyingType(Type) != null) target = typeof(Nullable<>).MakeGenericType(target);
                    value = Expression.Convert(value, target);
                }
                return Expression.Lambda<Func<object, object?>>(Expression.Convert(value, typeof(object)), input).Compile();
            });
        }
    }

    internal static class MemberSchema
    {
        internal static MemberColumn[] Infer(Type rowType, string? selector)
        {
            var parameter = Expression.Parameter(rowType, "row");
            var row = parameter;
            var paths = string.IsNullOrWhiteSpace(selector) ? Array.Empty<string>() : ExpressionHelper.SelectMemberNames(selector).ToArray();
            if (paths.Length > 0)
            {
                if (paths.Distinct(StringComparer.Ordinal).Count() != paths.Length)
                    throw new InvalidOperationException("Selected column names must be unique.");
                return paths.Select(path =>
                {
                    try { return new MemberColumn(path, ExpressionHelper.MemberAccess(row, path), parameter); }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Selector path '{path}' could not be resolved on type '{rowType.FullName}'.", ex);
                    }
                }).ToArray();
            }
            if (IsScalar(rowType) || Nullable.GetUnderlyingType(rowType) != null || GetElementType(rowType) != null || rowType.IsEnum)
                return new[] { new MemberColumn("Value", row, parameter) };

            var properties = rowType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => p.GetMethod != null && p.GetMethod.IsPublic && p.GetIndexParameters().Length == 0).ToArray();
            if (properties.Length == 0)
                throw new NotSupportedException($"Type '{rowType.FullName}' has no readable public instance properties and is not a supported scalar type.");
            return properties.Select(p => new MemberColumn(p.Name, Expression.Property(row, p), parameter)).ToArray();
        }

        internal static bool IsScalar(Type type) => type == typeof(bool) || type == typeof(sbyte) || type == typeof(byte)
            || type == typeof(short) || type == typeof(ushort) || type == typeof(int) || type == typeof(uint)
            || type == typeof(long) || type == typeof(ulong) || type == typeof(float) || type == typeof(double)
            || type == typeof(decimal) || type == typeof(string) || type == typeof(byte[]) || type == typeof(DateTime)
            || type == typeof(TimeSpan) || type == typeof(Guid);

        internal static Type? GetElementType(Type type)
        {
            if (type == typeof(byte[])) return null;
            if (type.IsArray && type.GetArrayRank() == 1) return type.GetElementType();
            if (type.IsGenericType)
            {
                var definition = type.GetGenericTypeDefinition();
                if (definition == typeof(List<>) || definition == typeof(IList<>) || definition == typeof(IEnumerable<>))
                    return type.GetGenericArguments()[0];
            }
            return null;
        }
    }
}
