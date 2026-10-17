using System;
using System.Linq;
using System.Linq.Expressions;

namespace Bonsai.Apache.Common
{
    // Shared expression operations for compiled Arrow converters and Parquet buffers.
    internal static class TypedExpression
    {
        internal static Expression ReplaceParameter(Expression body, ParameterExpression parameter, Expression replacement)
            => new ParameterReplacement(parameter, replacement).Visit(body)!;

        internal static Expression ConvertValue(Expression value, Type targetType)
        {
            if (value.Type == targetType) return value;
            // Parquet stores enumerable/list members as typed array columns.
            if (targetType.IsArray && MemberSchema.GetElementType(value.Type) == targetType.GetElementType())
            {
                return Expression.Condition(Expression.Equal(value, Expression.Constant(null, value.Type)),
                    Expression.Constant(null, targetType),
                    Expression.Call(typeof(Enumerable), nameof(Enumerable.ToArray), new[] { targetType.GetElementType()! }, value));
            }
            // Includes enum-to-underlying and lifted nullable enum conversions.
            return Expression.Convert(value, targetType);
        }

        sealed class ParameterReplacement : ExpressionVisitor
        {
            readonly ParameterExpression parameter;
            readonly Expression replacement;
            internal ParameterReplacement(ParameterExpression parameter, Expression replacement)
            {
                this.parameter = parameter;
                this.replacement = replacement;
            }
            protected override Expression VisitParameter(ParameterExpression node) => node == parameter ? replacement : node;
        }
    }
}
