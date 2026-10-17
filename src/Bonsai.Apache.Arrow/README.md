# Bonsai.Apache.Arrow

Read and write Apache Arrow IPC files (Feather V2) in Bonsai.

## Writing

`ArrowWriter` records scalar values as a `Value` column, or public properties as
named columns, while passing the input through. Use `Selector` to choose members,
including nested paths such as `Position.X,Position.Y`.

- `FileName`, `Suffix`, and `Overwrite` control the output path.
- `BatchSize` sets the number of rows per batch (default 100,000).
- `EnableCompression` enables Zstandard compression.
- `Buffered` enables background writing (default true).
- `QueueCapacity` limits pending inputs (default 1,024); a full queue reports an error.
- `FlushInterval` flushes partial batches (default five seconds; zero disables it).
  Applies only to background writing.

Keep input values unchanged until written. Completion finalizes the file;
cancellation drains and closes it in the background.

## Reading

Set `ArrowReader.FileName` and configure `Columns`. One column produces scalar
values; multiple columns produce tuples. `BatchSize` controls how many rows are
decoded at a time (default 4,096). Install `Bonsai.Apache.Arrow.Design`
to edit columns or import them with **Load Schema**.

Supported types include numeric values, strings, byte arrays, dates, durations,
GUIDs, enums, nullable values, and lists.

## Record batches

`ArrowWriter` also accepts `RecordBatch` sequences, preserving their schema and
metadata. `ArrowBatchWriter<T>` can infer columns or accept a schema and a custom batch
converter. Both it and `ParquetBatchWriter<T>` expose `Write`, `Flush`, and
`Dispose`; call them serially.

Python can read the files with `pyarrow.ipc.open_file(path)` or
`pyarrow.feather.read_table(path)`.
