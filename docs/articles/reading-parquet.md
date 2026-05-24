---
uid: reading-parquet
---

# Reading parquet files

The package ships an abstract [`ParquetReader<TRow>`] base class rather than a
configurable runtime operator. Users declare a typed reader by subclassing the
base — once the subclass exists, it is automatically usable as a Bonsai
operator. This keeps the per-row decoding code statically typed and removes the
need for a runtime configuration editor.

## Declaring a reader

A reader subclass overrides two members:

- `Columns` — the list of columns the reader expects to find in the file, each
  declared with a name and an expected C# logical type.
- `CreateRowFactory` — returns a delegate that constructs one `TRow` from a
  decoded batch given a zero-based row index. The base class manages the read
  loop, cancellation, and per-batch lifecycle.

The following minimal subclass is the companion to the writer example in
[Writing parquet files](xref:writing-parquet). It reads back the single-column
file produced by that workflow:

```csharp
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
```

`CreateRowFactory` is invoked once per decoded batch. The factory closes over
the typed column arrays returned by `batch.Column<T>(name)`; the base class
guarantees those arrays remain valid until the next call to
`CreateRowFactory`, which makes the closure-and-index pattern safe and
allocation-free in the per-row hot path.

## A minimal reader

Once the extension above is compiled, it appears in the Bonsai toolbox just
like any built-in operator:

:::workflow
![A minimal parquet reader that replays the file written by the writer example](../workflows/reading-parquet-basic.bonsai)
:::

The pipeline is a single source operator: `LongSampleReader` reads each row
from the configured `FileName` and emits one `long` per row, one `OnNext` at
a time. The sequence completes when the end of the file is reached, or earlier
if the subscription is disposed.

## Reading multi-column rows

A reader for a multi-column file declares one binding per column and combines
the per-column values inside the row factory. The following example reads
two columns — a `double` value and a `long` timestamp — into a
`Tuple<double, long>`:

```csharp
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
```

The same pattern scales to any number of columns. Custom row types (POCOs,
records, or nested data structures) can be constructed inside the factory; the
base class is agnostic to how `TRow` is produced as long as the factory
returns one per index.

## Schema validation

Validation happens on subscription. For each declared `ColumnBinding`:

- The column must exist in the file. A missing column throws with a message
  naming the binding.
- The file's logical type for that column must be compatible with the binding's
  expected C# type. A mismatch throws with a diff describing both sides.

Columns present in the file but not declared in `Columns` are silently
ignored. This lets a reader subset a richer file without breaking when new
columns are added upstream.

## Batch size

The `BatchSize` property on the operator controls how many rows are decoded
into the typed arrays at a time. It is a decode-throughput knob — the operator
still emits one `OnNext` per row regardless of batch size. Larger batches
reduce per-batch overhead at the cost of higher peak memory; the default is
usually fine and only needs tuning for very large files or very wide rows.

<!-- Reference Style Links -->
[`ParquetReader<TRow>`]: xref:Bonsai.Parquet.ParquetReader`1
