using System;
using ParquetSharp;

namespace Bonsai.Parquet
{
    /// <summary>
    /// Describes a single output parquet column: its name, the ParquetSharp
    /// <see cref="ParquetSharp.Column"/> descriptor, and a delegate that extracts
    /// the column value from a source row.
    /// </summary>
    internal sealed class ColumnPlan
    {
        public string Name { get; }
        public Column Column { get; }
        public Func<object, object?> Accessor { get; }
        public Type LogicalSystemType => Column.LogicalSystemType;

        public ColumnPlan(string name, Column column, Func<object, object?> accessor)
        {
            Name = name;
            Column = column;
            Accessor = accessor;
        }
    }
}
