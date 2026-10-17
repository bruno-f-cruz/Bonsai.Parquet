using System;

namespace Bonsai.Apache.Common
{
    internal static class ColumnTypes
    {
        // Indexed by each format's public column type enum.
        internal static readonly Type[] Types = { typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort),
            typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal),
            typeof(string), typeof(byte[]), typeof(DateTime), typeof(TimeSpan), typeof(Guid) };
    }
}
