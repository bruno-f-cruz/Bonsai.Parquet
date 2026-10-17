---
uid: writing-parquet
---

# Writing parquet files

[`ParquetWriter`] records each input as a row in a Parquet file and passes the
sequence through unchanged.

## Example

This workflow records timer ticks as a single column named `Value`:

:::workflow
![A minimal parquet writer that records timer ticks](../workflows/writing-parquet-basic.bonsai)
:::

Set `FileName` to the output path. Use `Suffix` to generate unique file names,
or `Overwrite` to replace an existing file.

## Schema

Scalar inputs become a `Value` column. Compound inputs become one column per
public property. Supported types include numeric values, strings, byte arrays,
dates, durations, GUIDs, enums, nullable values, and lists.

Use `Selector` to choose columns:

- `X,Y` selects two properties.
- `Position.X,Position.Y` selects nested members, using the full paths as column names.

## Writer settings

| Property | Default | Description |
| --- | --- | --- |
| `CompressionMethod` | `Snappy` | Compression codec; also supports Uncompressed, Gzip, Brotli, and Zstd. |
| `BatchSize` | `100000` | Rows per row group. |
| `Buffered` | `true` | Write on a background worker. |
| `QueueCapacity` | `1024` | Maximum pending inputs; a full queue reports an error. |
| `FlushInterval` | `00:00:05` | Flush partial batches in background mode; zero disables it. |

Keep input values unchanged until written. Completion flushes pending rows and
finalizes the file. Cancellation drains and closes it in the background.
With `Buffered = false`, writes run inline and flush on size or completion.

A Parquet file needs its final footer to be readable; a process crash can leave
an unfinished file unreadable.

For direct C# use, `ParquetBatchWriter<T>` provides `Write`, `Flush`, and
`Dispose`, with inferred columns or an explicit schema and batch callback.

[`ParquetWriter`]: xref:Bonsai.Apache.Parquet.ParquetWriter
