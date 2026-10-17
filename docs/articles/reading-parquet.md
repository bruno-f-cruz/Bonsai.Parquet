---
uid: reading-parquet
---

# Reading parquet files

[`ParquetReader`] reads configured columns from a Parquet file. Reading runs on a
background thread and completes at the end of the file.

## Example

This workflow reads the timer ticks recorded in the
[writing example](xref:writing-parquet):

:::workflow
![A minimal parquet reader that reads timer ticks](../workflows/reading-parquet-concrete.bonsai)
:::

Set `FileName`, then open the **Columns** editor. Use **Load Schema** to import
columns from a file, or add them manually.

| Property | Description |
| --- | --- |
| `Name` | Column name in the file. |
| `Type` | Scalar element type. |
| `IsNullable` | Read nullable values. |
| `IsArray` | Read a list column as an array. |

One column produces scalar values; multiple columns produce tuples. Only the
selected columns are read. Missing columns or incompatible types report an error.
`BatchSize` controls how many rows are decoded at a time.

[`ParquetReader`]: xref:Bonsai.Apache.Parquet.ParquetReader
