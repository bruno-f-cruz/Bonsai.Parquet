using System.IO;
using global::Apache.Arrow.Compression;

namespace Bonsai.Apache.Arrow.Design
{
    /// <summary>Edits Arrow schemas and imports columns from Arrow IPC files.</summary>
    public sealed class ArrowSchemaEditor : SchemaEditor
    {
        /// <inheritdoc/>
        protected override string FormatName => "Arrow";
        /// <inheritdoc/>
        protected override string FileFilter => "Arrow files (*.arrow;*.feather)|*.arrow;*.feather|All files (*.*)|*.*";
        /// <inheritdoc/>
        protected override ColumnDefinition[] LoadColumns(string path)
        {
            using var stream = File.OpenRead(path);
            using var reader = new global::Apache.Arrow.Ipc.ArrowFileReader(stream, new CompressionCodecFactory());
            return ConverterHelper.InferColumns(reader.Schema);
        }
    }
}
