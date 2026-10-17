using System;
using ParquetSharp;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Parquet
{
    /// <summary>
    /// Describes a single output parquet column: its name, the ParquetSharp
    /// <see cref="ParquetSharp.Column"/> descriptor, and its typed member expression.
    /// </summary>
    internal sealed class ColumnPlan
    {
        public string Name => Member.Name;
        public Column Column { get; }
        public MemberColumn Member { get; }
        public Type LogicalSystemType => Column.LogicalSystemType;

        public ColumnPlan(MemberColumn member, Column column)
        {
            Column = column;
            Member = member;
        }
    }
}
