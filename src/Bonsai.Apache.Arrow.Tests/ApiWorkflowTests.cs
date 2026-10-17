using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Xml;
using Bonsai.Expressions;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class ApiWorkflowTests
    {
        [TestMethod]
        [TestCategory("Workflow")]
        [DataRow("01 Scalar integers")]
        [DataRow("02 Named records")]
        [DataRow("03 Nested member selectors")]
        [DataRow("04 Nullable integers")]
        [DataRow("05 Variable length arrays")]
        [DataRow("06 Binary byte arrays")]
        [DataRow("07 DateTime timestamps")]
        [DataRow("08 Compressed mixed types")]
        [DataRow("09 Empty files")]
        [DataRow("All cases")]
        public void SavedWorkflowReturnsTrue(string name)
        {
            var directory = Path.Combine(Path.GetTempPath(), "Bonsai.Apache.Workflow", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            try
            {
                using var xml = XmlReader.Create(Path.Combine(AppContext.BaseDirectory, "Workflows", "api-tests.bonsai"));
                var workflow = (WorkflowBuilder)WorkflowBuilder.Serializer.Deserialize(xml)!;
                RedirectFiles(workflow.Workflow, directory);
                var graph = name == "All cases" ? workflow.Workflow : workflow.Workflow
                    .Select(node => node.Value).OfType<GroupWorkflowBuilder>()
                    .Single(group => group.Name == name).Workflow;

                var results = graph.BuildObservable<bool>()
                    .Timeout(TimeSpan.FromSeconds(30)).ToArray().Wait();
                CollectionAssert.AreEqual(new[] { true }, results, name + " must emit exactly one true result.");
            }
            finally
            {
                // Reader tasks can still be unwinding their native file handles
                // immediately after the observable reports completion.
                var deleted = SpinWait.SpinUntil(() =>
                {
                    try { Directory.Delete(directory, recursive: true); return true; }
                    catch (IOException) { return false; }
                }, TimeSpan.FromSeconds(5));
                if (!deleted) Directory.Delete(directory, recursive: true);
            }
        }

        static void RedirectFiles(ExpressionBuilderGraph graph, string directory)
        {
            foreach (var node in graph)
            {
                if (node.Value is GroupWorkflowBuilder group)
                    RedirectFiles(group.Workflow, directory);

                var element = node.Value is CombinatorBuilder builder ? builder.Combinator : node.Value;
                string Redirect(string fileName) => Path.Combine(directory, Path.GetFileName(fileName));
                switch (element)
                {
                    case ArrowWriter writer: writer.FileName = Redirect(writer.FileName); break;
                    case ArrowReader reader: reader.FileName = Redirect(reader.FileName); break;
                    case Parquet.ParquetWriter writer: writer.FileName = Redirect(writer.FileName); break;
                    case Parquet.ParquetReader reader: reader.FileName = Redirect(reader.FileName); break;
                }
            }
        }
    }
}
