using ParquetSharp;

namespace Bonsai.Apache.Parquet.Design
{
    /// <summary>Edits Parquet schemas and imports columns from Parquet files.</summary>
    public class ParquetSchemaEditor : SchemaEditor
    {
        /// <inheritdoc/>
        protected override string FormatName => "Parquet";
        /// <inheritdoc/>
        protected override string FileFilter => "Parquet files (*.parquet)|*.parquet|All files (*.*)|*.*";
        /// <inheritdoc/>
        protected override ColumnDefinition[] LoadColumns(string path)
        {
            using var reader = new ParquetFileReader(path);
            return ConverterHelper.InferColumns(reader.FileMetaData.Schema);
        }
    }
}
