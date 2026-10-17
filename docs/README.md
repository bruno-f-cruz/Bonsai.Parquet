# Bonsai.Apache

Read and write [Apache Parquet](https://parquet.apache.org/) and
[Apache Arrow](https://arrow.apache.org/) IPC files in Bonsai.

## Features

- `ParquetWriter` and `ArrowWriter` record sequences with schemas inferred from
  the input type. Use `Selector` to choose members, including nested paths.
- `ParquetReader` and `ArrowReader` read configured columns as scalar values or
  tuples. Their schema editors can load column definitions from a file.
- Supports numeric types, strings, binary data, dates, durations, GUIDs, enums,
  nullable values, and lists.
- Background writing, configurable batch sizes, and compression.

## Installation

Install `Bonsai.Apache.Parquet` or `Bonsai.Apache.Arrow` through the Bonsai package
manager. Install the matching `.Design` package for the reader's schema editor.

See [Writing Parquet](articles/writing-parquet.md),
[Reading Parquet](articles/reading-parquet.md), and
[Writing and reading Arrow](articles/writing-arrow.md).

## Development

Open `Bonsai.Apache.sln`, set `Bonsai.Apache` as the startup project, and press F5 to try
both packages in the test workflow.

## License

[MIT](https://licenses.nuget.org/MIT).
