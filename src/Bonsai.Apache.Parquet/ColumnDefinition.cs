using System;
using System.ComponentModel;

using Bonsai.Apache.Common;

namespace Bonsai.Apache.Parquet
{
    /// <summary>Defines a named column and its expected CLR type.</summary>
    public class ColumnDefinition
    {

        /// <summary>Gets or sets the unique column name.</summary>
        [DefaultValue("")]
        [Description("The name of the column. Must be unique within the schema.")]
        public string Name { get; set; } = string.Empty;

        /// <summary>Gets or sets the data type.</summary>
        [Description("The data type of the column.")]
        [DefaultValue(ParquetColumnType.Double)]
        public ParquetColumnType Type { get; set; } = ParquetColumnType.Double;

        /// <summary>Gets or sets whether the column can contain null values.</summary>
        [DefaultValue(false)]
        public bool IsNullable { get; set; }

        /// <summary>Gets or sets whether each row contains an array of values.</summary>
        [DefaultValue(false)]
        public bool IsArray { get; set; }

        /// <summary>Returns the corresponding CLR column type.</summary>
        public Type GetSystemType()
        {
            var index = Convert.ToInt32(Type);
            if (index < 0 || index >= ColumnTypes.Types.Length) throw new InvalidOperationException("Unknown column type.");
            var type = ColumnTypes.Types[index];
            if (IsArray) return type.MakeArrayType();
            return IsNullable && type.IsValueType ? typeof(Nullable<>).MakeGenericType(type) : type;
        }

        /// <inheritdoc/>
        public override string ToString() => $"{Name} ({Type}{(IsArray ? "[]" : IsNullable ? "?" : "")})";
    }
}
