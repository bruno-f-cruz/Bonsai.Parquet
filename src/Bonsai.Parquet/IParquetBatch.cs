namespace Bonsai.Parquet
{
    /// <summary>
    /// Provides typed access to a decoded batch of rows from a parquet file.
    /// </summary>
    /// <remarks>
    /// Arrays returned by <see cref="Column{T}(string)"/> are framework-owned, reused buffers.
    /// They are only valid during the current <c>ReadBatch</c> invocation; the first
    /// <see cref="RowCount"/> elements contain the decoded values for this batch.
    /// Callers must not retain references to these arrays or their contents across calls.
    /// </remarks>
    public interface IParquetBatch
    {
        /// <summary>Gets the number of valid rows in the current batch.</summary>
        int RowCount { get; }

        /// <summary>
        /// Returns the framework-owned buffer for the named column, cast to <typeparamref name="T"/>.
        /// Only indices <c>0</c> through <c>RowCount - 1</c> contain valid data.
        /// </summary>
        /// <typeparam name="T">The declared logical type for this column (must match the binding).</typeparam>
        /// <param name="name">Column name as declared in the <see cref="ColumnBinding"/>.</param>
        T[] Column<T>(string name);
    }
}
