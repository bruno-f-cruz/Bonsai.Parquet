# Bonsai.Parquet

`Bonsai.Parquet` exposes [Apache Parquet](https://parquet.apache.org/) file I/O
as Bonsai workflow operators, so streaming observable sequences can be recorded
to (and replayed from) a compact, columnar, strongly-typed binary format
without leaving the workflow.

## Features

- `ParquetWriter` sink that writes each element of a sequence as a row in a
  parquet file, mirroring the shape of Bonsai's built-in `CsvWriter`: schema
  inferred from the source element type, member-path `Selector`,
  `PathSuffix`/`Overwrite` integration, and buffered row groups flushed on
  size or dispose.
- `ParquetReader<TRow>` abstract base class for declaring strongly-typed
  parquet sources by subclassing — overrides describe the expected columns and
  the per-batch row factory, and the base class handles the read loop,
  schema validation, and cancellation.
- Support for the common Bonsai-friendly C# types out of the box: the integer
  widths, `float`, `double`, `decimal`, `string`, `byte[]`, `DateTime`,
  `TimeSpan`, `Guid`, their `Nullable<T>` counterparts, arrays / `IList<T>` of
  those types as parquet `LIST`, and enums as their underlying integer.

## Installation

Install `Bonsai.Parquet` through the Bonsai package manager. The package
brings in `ParquetSharp` (the underlying managed wrapper over Apache Arrow's
parquet implementation) and its native binaries, so no additional setup is
required on a supported platform.

## Feedback & Contributing

`Bonsai.Parquet` is released as open source under the
[MIT license](https://licenses.nuget.org/MIT). Bug reports and contributions
are welcome at the GitHub repository.
