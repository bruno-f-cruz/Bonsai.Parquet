using System;
using System.Collections.Generic;
using System.ComponentModel;
using Bonsai.Parquet;

[Description("Reads a parquet file with a single Value (long) column into a sequence of long values.")]
public class LongSampleReader : ParquetReader<long>
{
    protected override IReadOnlyList<ColumnBinding> Columns => new[]
    {
        new ColumnBinding("Value", typeof(long)),
    };

    protected override Func<int, long> CreateRowFactory(IParquetBatch batch)
    {
        var values = batch.Column<long>("Value");
        return i => values[i];
    }
}
