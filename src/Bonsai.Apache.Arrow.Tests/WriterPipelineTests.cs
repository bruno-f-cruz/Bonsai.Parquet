using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Bonsai.Apache.Common;

namespace Bonsai.Apache.Arrow.Tests
{
    [TestClass]
    public class WriterPipelineTests
    {
        [TestMethod]
        public void OneWorkerPreservesOrderAndClosesBeforeCompletion()
        {
            var caller = Thread.CurrentThread.ManagedThreadId;
            var threads = new HashSet<int>();
            var written = new List<int>();
            bool closed = false;
            var output = WriterPipeline.Process(Observable.Range(0, 50), true, 64, TimeSpan.Zero,
                () =>
                {
                    threads.Add(Thread.CurrentThread.ManagedThreadId);
                    return new WriterSink<int>(value =>
                    {
                        threads.Add(Thread.CurrentThread.ManagedThreadId);
                        written.Add(value);
                    }, () => threads.Add(Thread.CurrentThread.ManagedThreadId),
                    () => { threads.Add(Thread.CurrentThread.ManagedThreadId); closed = true; });
                });
            var actual = output.Do(_ => { }, () => Assert.IsTrue(closed)).ToArray()
                .Timeout(TimeSpan.FromSeconds(5)).Wait();
            CollectionAssert.AreEqual(Enumerable.Range(0, 50).ToArray(), actual);
            CollectionAssert.AreEqual(actual, written);
            Assert.HasCount(1, threads);
            Assert.IsFalse(threads.Contains(caller));
        }

        [TestMethod]
        public void FullQueueDoesNotWaitForTheWriterAndReportsAnError()
        {
            using var source = new Subject<int>();
            using var entered = new ManualResetEventSlim();
            using var unblock = new ManualResetEventSlim();
            using var ended = new ManualResetEventSlim();
            var written = new List<int>();
            Exception? failure = null;
            var output = WriterPipeline.Process(source, true, 1, TimeSpan.Zero,
                () => new WriterSink<int>(value =>
                {
                    if (value == 1) { entered.Set(); Assert.IsTrue(unblock.Wait(TimeSpan.FromSeconds(5))); }
                    written.Add(value);
                }, () => { }, () => { }));
            using var subscription = output.Subscribe(_ => { }, ex => { failure = ex; ended.Set(); });
            try
            {
                source.OnNext(1);
                Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(5)));
                source.OnNext(2);
                Assert.IsTrue(Task.Run(() => source.OnNext(3)).Wait(TimeSpan.FromSeconds(1)),
                    "A full queue must not block the acquisition thread.");
                unblock.Set();
                Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
                Assert.IsInstanceOfType<InvalidOperationException>(failure);
                CollectionAssert.AreEqual(new[] { 1, 2 }, written);
            }
            finally { unblock.Set(); }
        }

        [TestMethod]
        public void FlushFailuresAreReportedWhileTheSourceIsIdle()
        {
            using var source = new Subject<int>();
            using var ended = new ManualResetEventSlim();
            var expected = new IOException("flush failed");
            bool closed = false;
            Exception? failure = null;
            var output = WriterPipeline.Process(source, true, 64, TimeSpan.FromMilliseconds(20),
                () => new WriterSink<int>(_ => { }, () => throw expected, () => closed = true));
            using var subscription = output.Subscribe(_ => { }, ex => { failure = ex; ended.Set(); });
            Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
            Assert.AreSame(expected, failure);
            Assert.IsTrue(closed);
        }

        [TestMethod]
        public void DisposalReturnsWhileDiskIsBusyAndAcceptedWritesDrainAfterwards()
        {
            using var source = new Subject<int>();
            using var entered = new ManualResetEventSlim();
            using var unblock = new ManualResetEventSlim();
            using var closed = new ManualResetEventSlim();
            var written = new List<int>();
            int notifications = 0;
            var output = WriterPipeline.Process(source, true, 64, TimeSpan.Zero,
                () => new WriterSink<int>(value =>
                {
                    if (value == 1) { entered.Set(); Assert.IsTrue(unblock.Wait(TimeSpan.FromSeconds(5))); }
                    written.Add(value);
                }, () => { }, closed.Set));
            var subscription = output.Subscribe(_ => Interlocked.Increment(ref notifications));
            try
            {
                source.OnNext(1);
                Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(5)));
                source.OnNext(2);
                source.OnNext(3);
                Assert.IsTrue(Task.Run(subscription.Dispose).Wait(TimeSpan.FromSeconds(1)),
                    "Stopping must not join a worker waiting on disk I/O.");
                Assert.IsFalse(closed.IsSet);
                unblock.Set();
                Assert.IsTrue(closed.Wait(TimeSpan.FromSeconds(5)));
                Assert.AreEqual(0, notifications);
                CollectionAssert.AreEqual(new[] { 1, 2, 3 }, written);
            }
            finally { unblock.Set(); subscription.Dispose(); }
        }

        [TestMethod]
        public void WorkerFailureReleasesRetainedItemsAndReportsWithoutAnotherInput()
        {
            using var source = new Subject<int>();
            using var entered = new ManualResetEventSlim();
            using var unblock = new ManualResetEventSlim();
            using var ended = new ManualResetEventSlim();
            var released = new List<int>();
            var expected = new IOException("write failed");
            Exception? failure = null;
            bool closed = false;
            var output = WriterPipeline.Process(source, true, 64, TimeSpan.Zero,
                () => new WriterSink<int>(_ =>
                {
                    entered.Set();
                    Assert.IsTrue(unblock.Wait(TimeSpan.FromSeconds(5)));
                    throw expected;
                }, () => { }, () => closed = true), value => value, released.Add);
            using var subscription = output.Subscribe(_ => Assert.Fail("Failed item must not pass through."),
                ex => { failure = ex; ended.Set(); });
            try
            {
                source.OnNext(1);
                Assert.IsTrue(entered.Wait(TimeSpan.FromSeconds(5)));
                source.OnNext(2);
                source.OnNext(3);
                unblock.Set();
                Assert.IsTrue(ended.Wait(TimeSpan.FromSeconds(5)));
                Assert.AreSame(expected, failure);
                Assert.IsTrue(closed);
                CollectionAssert.AreEqual(new[] { 1, 2, 3 }, released);
            }
            finally { unblock.Set(); }
        }

        [TestMethod]
        public void SourceErrorsDrainAndCloseBeforeBeingReported()
        {
            var expected = new InvalidOperationException("source failed");
            var written = new List<int>();
            bool closed = false;
            var source = Observable.Range(1, 3).Concat(Observable.Throw<int>(expected));
            var output = WriterPipeline.Process(source, true, 64, TimeSpan.Zero,
                () => new WriterSink<int>(written.Add, () => { }, () => closed = true));
            var actual = Assert.ThrowsExactly<InvalidOperationException>(() => output
                .Timeout(TimeSpan.FromSeconds(5)).Wait());
            Assert.AreSame(expected, actual);
            Assert.IsTrue(closed);
            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, written);
        }
    }
}
