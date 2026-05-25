using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Bonsai;
using Bonsai.Expressions;
using ParquetSharp;

namespace Bonsai.Parquet
{
    [WorkflowElementCategory(ElementCategory.Source)]
    [Description("Reads a Parquet file and emits rows as typed values based on the configured column schema.")]
    public class ParquetReader : ExpressionBuilder
    {
        static readonly Range<int> argumentRange = new Range<int>(0, 0);
        public override Range<int> ArgumentRange => argumentRange;

        [Description("The path of the input Parquet file.")]
        [Editor("Bonsai.Design.OpenFileNameEditor, Bonsai.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public string? FileName { get; set; }

        [Description("Number of rows decoded per batch. Throughput knob only; output is always one OnNext per row.")]
        [DefaultValue(4096)]
        public int BatchSize { get; set; } = 4096;

        [Description("The columns to read from the Parquet file and their expected types.")]
        [Editor("Bonsai.Parquet.Design.ParquetSchemaEditor, Bonsai.Parquet.Design", "System.Drawing.Design.UITypeEditor, System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a")]
        public Collection<ColumnDefinition> Columns { get; set; } = new Collection<ColumnDefinition>();

        public override Expression Build(IEnumerable<Expression> arguments)
        {
            var columns = Columns?.ToArray() ?? Array.Empty<ColumnDefinition>();
            if (columns.Length == 0)
                throw new InvalidOperationException("ParquetReader requires at least one column.");

            var outputType = ComputeOutputType(columns);
            var generateMethod = GetType()
                .GetMethod(nameof(Generate), BindingFlags.Instance | BindingFlags.Public)!
                .MakeGenericMethod(outputType);

            return Expression.Call(Expression.Constant(this), generateMethod);
        }

        static Type ComputeOutputType(ColumnDefinition[] columns)
        {
            if (columns.Length == 1) return columns[0].GetSystemType();
            return MakeTupleType(columns.Select(c => c.GetSystemType()).ToArray());
        }

        static readonly Type?[] OpenTupleTypes =
        {
            null,           // 0 — unused
            null,           // 1 — handled as scalar
            typeof(Tuple<,>),
            typeof(Tuple<,,>),
            typeof(Tuple<,,,>),
            typeof(Tuple<,,,,>),
            typeof(Tuple<,,,,,>),
            typeof(Tuple<,,,,,,>),
            typeof(Tuple<,,,,,,,>), // 8-arg: T1..T7 + TRest
        };

        static Type MakeTupleType(Type[] types)
        {
            if (types.Length <= 7)
                return OpenTupleTypes[types.Length]!.MakeGenericType(types);
            // 8+: Tuple<T1,...,T7, Tuple<T8,...>>
            var rest = MakeTupleType(types.Skip(7).ToArray());
            var all8 = types.Take(7).Append(rest).ToArray();
            return OpenTupleTypes[8]!.MakeGenericType(all8);
        }

        static Func<Array[], int, TRow> BuildRowFactory<TRow>(ColumnDefinition[] columns)
        {
            var buffersParam = Expression.Parameter(typeof(Array[]), "b");
            var indexParam   = Expression.Parameter(typeof(int), "i");

            var body = BuildRowExpression(buffersParam, indexParam, columns, 0);
            var castBody = body.Type == typeof(TRow)
                ? body
                : (Expression)Expression.Convert(body, typeof(TRow));

            return Expression.Lambda<Func<Array[], int, TRow>>(castBody, buffersParam, indexParam).Compile();
        }

        static Expression BuildRowExpression(
            Expression buffersParam, Expression indexParam,
            ColumnDefinition[] columns, int startAt)
        {
            int remaining = columns.Length - startAt;

            if (remaining == 1)
                return BuildColumnAccess(buffersParam, indexParam, columns[startAt], startAt);

            int arity = Math.Min(remaining, 7);
            var argExprs = new Expression[arity];
            for (int j = 0; j < arity; j++)
                argExprs[j] = BuildColumnAccess(buffersParam, indexParam, columns[startAt + j], startAt + j);

            if (remaining <= 7)
            {
                var argTypes = argExprs.Select(e => e.Type).ToArray();
                return Expression.Call(FindTupleCreate(argTypes), argExprs);
            }
            else
            {
                // >7 columns: nest as Tuple<T1,...,T7, Tuple<T8,...>>
                var restExpr = BuildRowExpression(buffersParam, indexParam, columns, startAt + 7);
                var all8 = argExprs.Append(restExpr).ToArray();
                return Expression.Call(FindTupleCreate(all8.Select(e => e.Type).ToArray()), all8);
            }
        }

        static Expression BuildColumnAccess(
            Expression buffersParam, Expression indexParam,
            ColumnDefinition col, int bufferIndex)
        {
            var elemType  = col.GetSystemType();
            // ((elemType[])buffers[bufferIndex])[i]
            var arrayElem  = Expression.ArrayIndex(buffersParam, Expression.Constant(bufferIndex));
            var typedArray = Expression.Convert(arrayElem, elemType.MakeArrayType());
            return Expression.ArrayIndex(typedArray, indexParam);
        }

        static MethodInfo FindTupleCreate(Type[] argTypes)
        {
            return typeof(Tuple)
                .GetMethods(BindingFlags.Static | BindingFlags.Public)
                .First(m => m.Name == "Create" && m.GetParameters().Length == argTypes.Length)
                .MakeGenericMethod(argTypes);
        }

        public IObservable<TRow> Generate<TRow>()
        {
            return Observable.Create<TRow>(observer =>
            {
                var cts = new CancellationTokenSource();

                Task.Run(() =>
                {
                    try
                    {
                        var fileName = FileName;
                        if (string.IsNullOrEmpty(fileName))
                            throw new InvalidOperationException("A valid file path must be specified.");
                        if (!File.Exists(fileName))
                            throw new FileNotFoundException($"Parquet file not found: '{fileName}'.", fileName);

                        var batchSize = BatchSize;
                        if (batchSize <= 0)
                            throw new InvalidOperationException("BatchSize must be greater than zero.");

                        var columnDefs = Columns?.ToArray() ?? Array.Empty<ColumnDefinition>();
                        if (columnDefs.Length == 0)
                            throw new InvalidOperationException("Columns must contain at least one entry.");

                        var bindings = columnDefs
                            .Select(c => new ColumnBinding(c.Name, c.GetSystemType()))
                            .ToList();

                        var seen = new HashSet<string>(StringComparer.Ordinal);
                        foreach (var b in bindings)
                            if (!seen.Add(b.Name))
                                throw new InvalidOperationException($"Duplicate column name: '{b.Name}'.");

                        // Compiled once at subscribe time; the expression tree builds the typed
                        // array accesses so there is no boxing or dictionary lookup per row.
                        var rowFactory = BuildRowFactory<TRow>(columnDefs);

                        using var fileReader = new ParquetFileReader(fileName);
                        var schema = fileReader.FileMetaData.Schema;

                        // ColumnRoot gives the top-level field name (same reason as AbstractParquetReader).
                        var fileColumnMap = new Dictionary<string, int>(schema.NumColumns, StringComparer.Ordinal);
                        for (int i = 0; i < schema.NumColumns; i++)
                            fileColumnMap[schema.ColumnRoot(i).Name] = i;

                        var columnIndices = new int[bindings.Count];
                        var buffers       = new Array[bindings.Count];

                        for (int i = 0; i < bindings.Count; i++)
                        {
                            if (!fileColumnMap.TryGetValue(bindings[i].Name, out int colIdx))
                            {
                                var available = string.Join(", ", fileColumnMap.Keys);
                                throw new InvalidOperationException(
                                    $"Column '{bindings[i].Name}' not found in file. Available: {available}");
                            }
                            columnIndices[i] = colIdx;
                            buffers[i] = Array.CreateInstance(bindings[i].LogicalType, batchSize);
                        }

                        if (fileReader.FileMetaData.NumRowGroups > 0)
                        {
                            using var vRg = fileReader.RowGroup(0);
                            for (int i = 0; i < bindings.Count; i++)
                            {
                                using var ur = vRg.Column(columnIndices[i]).LogicalReader();
                                var rType = ur.GetType();
                                var actual = rType.IsGenericType ? rType.GetGenericArguments()[0] : null;
                                if (actual != bindings[i].LogicalType)
                                    throw new InvalidOperationException(
                                        $"Column '{bindings[i].Name}': expected '{bindings[i].LogicalType.FullName}', file has '{actual?.FullName ?? "unknown"}'.");
                            }
                        }

                        // MakeGenericMethod once per column, not per batch.
                        var openMethod = typeof(ColumnDecoder)
                            .GetMethod("Open", BindingFlags.Static | BindingFlags.NonPublic)!;
                        var decoderFactories = new Func<RowGroupReader, ColumnDecoder>[bindings.Count];
                        for (int i = 0; i < bindings.Count; i++)
                        {
                            var openGeneric  = openMethod.MakeGenericMethod(bindings[i].LogicalType);
                            var capturedBuf  = buffers[i];
                            var capturedIdx  = columnIndices[i];
                            decoderFactories[i] = rg =>
                                (ColumnDecoder)openGeneric.Invoke(null, new object[] { rg, capturedIdx, capturedBuf })!;
                        }

                        int numRowGroups = fileReader.FileMetaData.NumRowGroups;
                        for (int rg = 0; rg < numRowGroups; rg++)
                        {
                            if (cts.IsCancellationRequested) return;

                            using var rowGroupReader = fileReader.RowGroup(rg);
                            long numRowsInGroup = rowGroupReader.MetaData.NumRows;

                            var decoders = new ColumnDecoder[bindings.Count];
                            try
                            {
                                for (int c = 0; c < bindings.Count; c++)
                                    decoders[c] = decoderFactories[c](rowGroupReader);

                                long rowsRead = 0;
                                while (rowsRead < numRowsInGroup)
                                {
                                    if (cts.IsCancellationRequested) return;

                                    int count = (int)Math.Min(batchSize, numRowsInGroup - rowsRead);
                                    for (int c = 0; c < decoders.Length; c++)
                                        decoders[c].Read(count);

                                    for (int r = 0; r < count; r++)
                                    {
                                        if (cts.IsCancellationRequested) return;
                                        observer.OnNext(rowFactory(buffers, r));
                                    }

                                    rowsRead += count;
                                }
                            }
                            finally
                            {
                                for (int c = 0; c < decoders.Length; c++)
                                    decoders[c]?.Dispose();
                            }
                        }

                        if (!cts.IsCancellationRequested)
                            observer.OnCompleted();
                    }
                    catch (Exception ex)
                    {
                        if (!cts.IsCancellationRequested)
                            observer.OnError(ex);
                    }
                });

                return Disposable.Create(() => cts.Cancel());
            });
        }
    }
}
