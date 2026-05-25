---
uid: reading-parquet
---

# Reading parquet files

The package offers two ways to read a parquet file:

- **[`ParquetReader`]** — an editor-driven source operator. Configure columns
  entirely in the Bonsai property editor with no code required. Output type is
  inferred automatically from the column schema.
- **[`AbstractParquetReader<TRow>`]** — a code-first abstract base class. Write
  a short C# subclass to declare which columns to read and how to assemble rows
  into any `TRow` you choose. Best when you want named fields or a custom row
  type.

Both operators read on a background thread, emit one `OnNext` per row, and
complete when the file is exhausted.

---

## `ParquetReader` — editor-driven

### A minimal reader

The following workflow reads back the single-column file produced by the
[Writing parquet files](xref:writing-parquet) example:

:::workflow
![A minimal parquet reader that reads timer ticks](../workflows/reading-parquet-concrete.bonsai)
:::

Drop `ParquetReader` onto the workflow, set its `FileName` to an existing
`.parquet` file, and configure the columns schema.

### Configuring columns in the editor

Open the `Columns` property in the property panel (click the `…` button next
to `Columns`). The **Edit Parquet Schema** dialog provides:

- **Add** — adds a blank column entry.
- **Remove** — removes the selected column entry.
- **Load Schema** — opens a file picker; select any `.parquet` file and the
  editor closes and reopens pre-populated with every column from that file's
  schema. Use this to avoid typing column names and types by hand.

Each column entry exposes four properties:

| Property | Description |
|---|---|
| `Name` | Column name exactly as it appears in the parquet file. |
| `Type` | Scalar element type — see the [type table](#column-types) below. |
| `IsNullable` | When true, the column is read as `Nullable<T>` (`int?`, `double?`, …). Has no effect on reference types (`String`, `Bytes`). |
| `IsArray` | When true, the column is read as `T[]` — a parquet LIST column. Takes precedence over `IsNullable`. |

### Column types

`Type` is a `ParquetColumnType` enum value that identifies the scalar element
type of the column. The corresponding C# type used for decoding is:

| `ParquetColumnType` | C# type | Parquet physical / logical |
|---|---|---|
| `Bool` | `bool` | `BOOLEAN` |
| `SByte` | `sbyte` | `INT32 / INT(8, true)` |
| `Byte` | `byte` | `INT32 / INT(8, false)` |
| `Int16` | `short` | `INT32 / INT(16, true)` |
| `UInt16` | `ushort` | `INT32 / INT(16, false)` |
| `Int32` | `int` | `INT32` |
| `UInt32` | `uint` | `INT32 / INT(32, false)` |
| `Int64` | `long` | `INT64` |
| `UInt64` | `ulong` | `INT64 / INT(64, false)` |
| `Float` | `float` | `FLOAT` |
| `Double` | `double` | `DOUBLE` |
| `Decimal` | `decimal` | `FIXED_LEN_BYTE_ARRAY / DECIMAL` |
| `String` | `string` | `BYTE_ARRAY / STRING` |
| `Bytes` | `byte[]` | `BYTE_ARRAY` |
| `DateTime` | `DateTime` | `INT64 / TIMESTAMP(MICROS)` |
| `TimeSpan` | `TimeSpan` | `INT64 / TIME(MICROS)` |
| `Guid` | `Guid` | `FIXED_LEN_BYTE_ARRAY / UUID` |

`IsNullable = true` wraps the C# type in `Nullable<T>` (e.g. `int?`).
`IsArray = true` wraps it in `T[]` (e.g. `int[]`). Both flags together are not
supported — use `IsArray` for LIST columns; nullability of individual list
elements is not configurable from the editor.

### Output type

The output type emitted downstream depends on how many columns are configured:

| Columns | Output type |
|---|---|
| 1 | The column's scalar C# type (e.g. `long`) |
| 2 | `Tuple<T1, T2>` |
| 3 | `Tuple<T1, T2, T3>` |
| 4–7 | `Tuple<T1, …, T7>` |
| 8+ | Nested: `Tuple<T1, …, T7, Tuple<T8, …>>` |

For the single-column example above the output is `long`. Adding a second
column of type `Double` would change the output to `Tuple<long, double>`.
Downstream operators see the concrete type, not `object`.

---

## `AbstractParquetReader<TRow>` — code-first

Use this when you need a named output type, want C# value tuples, or need a row
type that does not map cleanly to a Bonsai `Tuple<…>`.

### Declaring a reader

A subclass overrides two members:

- `Columns` — the list of columns the reader expects to find in the file, each
  declared as a [`ColumnBinding`] with a name and an expected C# logical type.
- `CreateRowFactory` — called once per decoded batch; returns a
  `Func<int, TRow>` that constructs one `TRow` from a zero-based row index. The
  base class manages the read loop, cancellation, and per-batch lifecycle.

The following minimal subclass reads the single-column file produced by the
[Writing parquet files](xref:writing-parquet) example:

```csharp
using System;
using System.Collections.Generic;
using System.ComponentModel;
using Bonsai.Parquet;

[Description("Reads a parquet file with a single Value (long) column.")]
public class LongSampleReader : AbstractParquetReader<long>
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
the typed column arrays; the base class guarantees those arrays remain valid
until the next `CreateRowFactory` call, so the closure-and-index pattern is
both safe and allocation-free in the per-row hot path.

### Using the reader in a workflow

Compile the extension into the workflow's `Extensions` folder.
`LongSampleReader` then appears in the Bonsai toolbox just like any built-in
operator. Drop it onto the workflow, set its `FileName`, and it emits one
`long` per row.

### Choosing the row type (`TRow`)

`TRow` can be any C# type. Common patterns:

**Scalar** — when only one column is needed, use the column's type directly:

```csharp
public class TemperatureReader : AbstractParquetReader<double>
{
    protected override IReadOnlyList<ColumnBinding> Columns => new[]
    {
        new ColumnBinding("temperature_c", typeof(double)),
    };

    protected override Func<int, double> CreateRowFactory(IParquetBatch batch)
    {
        var temps = batch.Column<double>("temperature_c");
        return i => temps[i];
    }
}
```

**Value tuple** — idiomatic for multi-column rows with named fields:

```csharp
public class SensorReader : AbstractParquetReader<(DateTime Time, double Value, int Channel)>
{
    protected override IReadOnlyList<ColumnBinding> Columns => new[]
    {
        new ColumnBinding("time",    typeof(DateTime)),
        new ColumnBinding("value",  typeof(double)),
        new ColumnBinding("channel", typeof(int)),
    };

    protected override Func<int, (DateTime, double, int)> CreateRowFactory(IParquetBatch batch)
    {
        var t = batch.Column<DateTime>("time");
        var v = batch.Column<double>("value");
        var c = batch.Column<int>("channel");
        return i => (t[i], v[i], c[i]);
    }
}
```

**POCO** — use when downstream code expects a named class:

```csharp
public class SensorRow
{
    public DateTime Time { get; set; }
    public double Value { get; set; }
}

public class SensorPocoReader : AbstractParquetReader<SensorRow>
{
    protected override IReadOnlyList<ColumnBinding> Columns => new[]
    {
        new ColumnBinding("time",  typeof(DateTime)),
        new ColumnBinding("value", typeof(double)),
    };

    protected override Func<int, SensorRow> CreateRowFactory(IParquetBatch batch)
    {
        var t = batch.Column<DateTime>("time");
        var v = batch.Column<double>("value");
        return i => new SensorRow { Time = t[i], Value = v[i] };
    }
}
```

### Choosing the `LogicalType` for a `ColumnBinding`

The second argument to `ColumnBinding` is the C# type that ParquetSharp maps
the column to. It must match what was written (see the
[type table](#column-types) above for the mapping). Use `typeof(T?)` for
nullable columns and `typeof(T[])` for LIST columns:

| File column | `typeof(...)` |
|---|---|
| Non-null int | `typeof(int)` |
| Nullable int | `typeof(int?)` |
| Array of int | `typeof(int[])` |
| Non-null double | `typeof(double)` |
| Nullable double | `typeof(double?)` |
| String | `typeof(string)` |
| DateTime | `typeof(DateTime)` |
| Nullable DateTime | `typeof(DateTime?)` |
| Raw bytes | `typeof(byte[])` |
| Array of double | `typeof(double[])` |

---

## Schema validation

Both operators validate the schema on subscription. For each declared column:

- The column must exist in the file by name. A missing column throws with a
  message naming the binding and listing the columns that are present.
- The file's logical type must match the declared C# type. A mismatch throws
  with a diff of the expected and actual types.

Columns present in the file but not declared are silently ignored — a reader
can subset a richer file without breaking when new columns are added upstream.

## Batch size

The `BatchSize` property controls how many rows are decoded into memory at a
time. It is a decode-throughput knob — both operators still emit one `OnNext`
per row. Larger batches amortise per-batch overhead at the cost of higher peak
memory. The default of 4096 is suitable for most workflows.

<!-- Reference Style Links -->
[`ParquetReader`]: xref:Bonsai.Parquet.ParquetReader
[`AbstractParquetReader<TRow>`]: xref:Bonsai.Parquet.AbstractParquetReader`1
[`ColumnBinding`]: xref:Bonsai.Parquet.ColumnBinding
