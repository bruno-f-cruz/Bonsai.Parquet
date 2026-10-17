using System;
using System.Linq;
using global::Apache.Arrow;
using global::Apache.Arrow.Types;

namespace Bonsai.Apache.Arrow
{
    /// <summary>Converts Arrow file schemas to editable column definitions.</summary>
    public static class ConverterHelper
    {
        /// <summary>Infers supported column definitions from an Arrow schema.</summary>
        public static ColumnDefinition[] InferColumns(Schema schema) => schema.FieldsList.Select(field =>
        {
            var type = field.DataType;
            bool isArray = type is ListType;
            if (type is ListType list) type = list.ValueDataType;
            var columnType = type.TypeId switch
            {
                ArrowTypeId.Boolean => ArrowColumnType.Bool,
                ArrowTypeId.Int8 => ArrowColumnType.SByte,
                ArrowTypeId.UInt8 => ArrowColumnType.Byte,
                ArrowTypeId.Int16 => ArrowColumnType.Int16,
                ArrowTypeId.UInt16 => ArrowColumnType.UInt16,
                ArrowTypeId.Int32 => ArrowColumnType.Int32,
                ArrowTypeId.UInt32 => ArrowColumnType.UInt32,
                ArrowTypeId.Int64 => ArrowColumnType.Int64,
                ArrowTypeId.UInt64 => ArrowColumnType.UInt64,
                ArrowTypeId.Float => ArrowColumnType.Float,
                ArrowTypeId.Double => ArrowColumnType.Double,
                ArrowTypeId.Decimal128 => ArrowColumnType.Decimal,
                ArrowTypeId.String => ArrowColumnType.String,
                ArrowTypeId.Binary => ArrowColumnType.Bytes,
                ArrowTypeId.Timestamp => ArrowColumnType.DateTime,
                ArrowTypeId.Duration => ArrowColumnType.TimeSpan,
                _ => throw new NotSupportedException($"Column '{field.Name}' has unsupported Arrow type '{type.Name}'.")
            };
            return new ColumnDefinition { Name = field.Name, Type = columnType, IsArray = isArray, IsNullable = field.IsNullable };
        }).ToArray();

    }
}
