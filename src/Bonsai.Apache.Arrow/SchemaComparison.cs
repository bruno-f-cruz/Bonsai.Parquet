using System.IO;
using System.Linq;
using global::Apache.Arrow;
using global::Apache.Arrow.Ipc;
using global::Apache.Arrow.Types;

namespace Bonsai.Apache.Arrow
{
    internal static class SchemaComparison
    {
        // Arrow schema/type Equals uses reference equality. Compare IPC schemas to cover
        // nested types, dictionaries, run-end encoding and metadata as well as primitives.
        internal static byte[] Serialize(Schema schema)
        {
            using var stream = new MemoryStream();
            using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true)) writer.WriteStart();
            return stream.ToArray();
        }

        internal static bool TypeEquals(IArrowType actual, IArrowType expected)
        {
            if (ReferenceEquals(actual, expected)) return true;
            return Serialize(new Schema(new[] { new Field("Value", actual, true) }, null)).SequenceEqual(
                Serialize(new Schema(new[] { new Field("Value", expected, true) }, null)));
        }
    }
}
