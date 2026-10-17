namespace Bonsai.Apache.Arrow
{
    /// <summary>Identifies the C# logical type of a column in a <see cref="ArrowReader"/> schema.</summary>
    public enum ArrowColumnType
    {
        /// <summary>The Bool column type.</summary>
        Bool,
        /// <summary>The SByte column type.</summary>
        SByte,
        /// <summary>The Byte column type.</summary>
        Byte,
        /// <summary>The Int16 column type.</summary>
        Int16,
        /// <summary>The UInt16 column type.</summary>
        UInt16,
        /// <summary>The Int32 column type.</summary>
        Int32,
        /// <summary>The UInt32 column type.</summary>
        UInt32,
        /// <summary>The Int64 column type.</summary>
        Int64,
        /// <summary>The UInt64 column type.</summary>
        UInt64,
        /// <summary>The Float column type.</summary>
        Float,
        /// <summary>The Double column type.</summary>
        Double,
        /// <summary>The Decimal column type.</summary>
        Decimal,
        /// <summary>The String column type.</summary>
        String,
        /// <summary>The Bytes column type.</summary>
        Bytes,      // byte[]
        /// <summary>The DateTime column type.</summary>
        DateTime,
        /// <summary>The TimeSpan column type.</summary>
        TimeSpan,
        /// <summary>The Guid column type.</summary>
        Guid,
    }
}
