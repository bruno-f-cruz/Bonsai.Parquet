namespace Bonsai.Parquet
{
    /// <summary>Identifies the C# logical type of a column in a <see cref="ParquetReader"/> schema.</summary>
    public enum ParquetColumnType
    {
        Bool,
        SByte,
        Byte,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Float,
        Double,
        Decimal,
        String,
        Bytes,      // byte[]
        DateTime,
        TimeSpan,
        Guid,
    }
}
