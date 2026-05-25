using System;
using System.ComponentModel;

namespace Bonsai.Parquet
{
    /// <summary>Defines a single column in a <see cref="ParquetReader"/> schema.</summary>
    public class ColumnDefinition
    {
        [DefaultValue("")]
        [Description("The name of the column. Must be unique within the schema.")]
        public string Name { get; set; } = string.Empty;

        [DefaultValue(ParquetColumnType.Double)]
        [Description("The data type of the column.")]
        public ParquetColumnType Type { get; set; } = ParquetColumnType.Double;

        [DefaultValue(false)]
        [Description("Indicates whether the column can contain null values.")]
        public bool IsNullable { get; set; }

        [DefaultValue(false)]
        [Description("Indicates whether the column is an array.")]
        public bool IsArray { get; set; }

        /// <summary>Returns the System.Type that ParquetSharp uses for this column.</summary>
        public Type GetSystemType()
        {
            var baseType = ConverterHelper.ToCsharpType(Type);
            if (IsArray)
                return baseType.MakeArrayType();
            // Reference types (string, byte[]) are already nullable; don't wrap in Nullable<T>
            if (IsNullable && baseType.IsValueType)
                return typeof(Nullable<>).MakeGenericType(baseType);
            return baseType;
        }

        public override string ToString()
        {
            var suffix = IsArray ? "[]" : IsNullable ? "?" : "";
            return $"{Name} ({Type}{suffix})";
        }
    }
}
