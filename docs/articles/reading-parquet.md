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

## `ParquetReader`

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

Note: some parquet types may not yet be supported by `ParquetReader`'s editor-driven configuration. In that case, use `AbstractParquetReader<TRow>` for full control over the column schema.


### Output type

If a single column is selected, the operator will simply emit an Observable of that column's type. Otherwise, if multiple columns are selected, the output will be flattened into a `Tuple<...>` type with one element per column. The column types are inferred from the schema and the `IsNullable`/`IsArray` settings. For the single-column example above the output is `long`.

---

## `AbstractParquetReader<TRow>`

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
memory.

<!-- Reference Style Links -->
[`ParquetReader`]: xref:Bonsai.Parquet.ParquetReader
[`AbstractParquetReader<TRow>`]: xref:Bonsai.Parquet.AbstractParquetReader`1
[`ColumnBinding`]: xref:Bonsai.Parquet.ColumnBinding
