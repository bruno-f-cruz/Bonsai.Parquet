using System;
using System.Collections.Generic;
using System.ComponentModel;
using Bonsai.Parquet;

[Description("Reads a parquet file with Value (double) and Timestamp (long) columns into Tuple<double, long> rows.")]
public class TimestampedSampleReader : ParquetReader<Tuple<double, long>>
{
    protected override IReadOnlyList<ColumnBinding> Columns => new[]
    {
        new ColumnBinding("Value", typeof(double)),
        new ColumnBinding("Timestamp", typeof(long)),
    };

    protected override Func<int, Tuple<double, long>> CreateRowFactory(IParquetBatch batch)
    {
        var values = batch.Column<double>("Value");
        var timestamps = batch.Column<long>("Timestamp");
        return i => Tuple.Create(values[i], timestamps[i]);
    }
}
