using System;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Describes a single column that a <see cref="ParquetReader{TRow}"/> subclass wants to read,
    /// pairing the column name in the file with the expected C# logical type.
    /// </summary>
    public readonly struct ColumnBinding
    {
        /// <summary>Gets the column name as it appears in the parquet file schema.</summary>
        public string Name { get; }

        /// <summary>
        /// Gets the expected C# logical type for this column (e.g. <c>typeof(int)</c>,
        /// <c>typeof(int?)</c>, <c>typeof(int[])</c>, <c>typeof(string)</c>).
        /// Must match the type the file was written with.
        /// </summary>
        public Type LogicalType { get; }

        /// <summary>
        /// Initialises a new <see cref="ColumnBinding"/>.
        /// </summary>
        /// <param name="name">Column name in the parquet file. Must not be null or empty.</param>
        /// <param name="logicalType">Expected C# logical type. Must not be null.</param>
        public ColumnBinding(string name, Type logicalType)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Column name cannot be null or empty.", nameof(name));
            if (logicalType == null)
                throw new ArgumentNullException(nameof(logicalType));
            Name = name;
            LogicalType = logicalType;
        }
    }
}
