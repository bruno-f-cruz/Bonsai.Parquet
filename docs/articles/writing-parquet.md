---
uid: writing-parquet
---

# Writing parquet files

The [`ParquetWriter`] operator writes each element of an observable sequence as
a row in a [Apache Parquet](https://parquet.apache.org/) file. It mirrors the
shape of Bonsai's built-in [`CsvWriter`]: a sink that opens the file on
subscription, infers a column schema from the source element type, buffers rows
in memory, and flushes them to disk in row groups.

## A minimal writer

The following workflow writes a sequence of `long` ticks emitted by a
[`Timer`] into a parquet file:

:::workflow
![A minimal parquet writer that records timer ticks](../workflows/writing-parquet-basic.bonsai)
:::

The pipeline:

1. **The source.** [`Timer`] emits an incrementing `long` once per period.
2. **The sink.** [`ParquetWriter`] receives each value. Because `long` is a
   supported scalar type, the inferred schema is a single column named `Value`.
   The file path is set on the operator's `FileName` property.

The writer is a passthrough sink: the source sequence is forwarded unchanged so
the same stream can be observed or processed downstream of the writer.

## Schema inference

The column layout is derived from the static type of the source sequence at
subscription time. Two cases:

- **Scalar source types** (`bool`, the integer widths, `float`, `double`,
  `decimal`, `string`, `byte[]`, `DateTime`, `TimeSpan`, `Guid`, their
  `Nullable<T>` counterparts, arrays / `IList<T>` of those, and enums) become a
  single column named `Value`.
- **Compound source types** become one column per public instance property, in
  declaration order, with each column named after the property.

Nested objects (parquet `GROUP` types) are not supported. Flatten them with
the `Selector` property described below.

## Selecting members

The `Selector` property accepts the same comma-separated member-path syntax as
[`CsvWriter`]. Each path becomes one column in the output file:

- `X,Y` writes two columns named `X` and `Y` from the matching properties of
  the source element.
- `Position.X,Position.Y` flattens a nested `Position` member into two columns
  named `Position.X` and `Position.Y`.

The selector column names use the full dotted path. This differs from
`CsvWriter`'s header convention, which strips the parameter prefix and the
leaf name: parquet column names must be unique within a file, so the full path
is the only sound mapping when two members share a leaf name (for example
`Position.X` and `Velocity.X`).

The `Selector` editor in the property grid is provided by `Bonsai.System.Design`.
Make sure that package is loaded in your Bonsai environment to get the member
picker dialog.

## Compression and row groups

Two properties control how the file is laid out on disk:

- `CompressionMethod` selects the codec applied to each row group. The default
  is `Snappy`, which trades a small amount of size for fast encode/decode.
  Other supported codecs include `Gzip`, `Zstd`, `Brotli`, and `Lz4`.
- `RowGroupSize` is the number of buffered rows that triggers a flush to disk.
  The default of 100&nbsp;000 is a reasonable starting point for streaming
  workflows; lower values reduce peak memory at the cost of a less compact
  file.

A trailing partial row group is always flushed when the operator is disposed,
either through normal workflow shutdown or downstream unsubscription.

## File path conventions

The writer uses Bonsai's standard `Bonsai.IO` path utilities:

- `FileName` is the output path. Intermediate directories are created if they
  do not exist.
- `Suffix` appends a generated suffix (timestamp, file count, etc.) to the
  file stem so repeated runs do not collide.
- `Overwrite` controls whether an existing file at the resolved path is
  replaced. The default is `false`, in which case the writer throws on
  subscription if the file already exists.

## Crash safety

A parquet file is only readable once its footer is written. The writer emits
the footer on `Dispose`, which covers normal workflow stop and Rx
unsubscription. A hard process crash leaves the file unreadable — this is a
limitation of the parquet format itself, not of this operator. Workflows that
need crash-resistant recordings should compose the writer with a file-rotation
strategy upstream (for example, a new file per minute via the standard
windowing operators).

<!-- Reference Style Links -->
[`ParquetWriter`]: xref:Bonsai.Parquet.ParquetWriter
[`CsvWriter`]: xref:Bonsai.IO.CsvWriter
[`Timer`]: xref:Bonsai.Reactive.Timer
