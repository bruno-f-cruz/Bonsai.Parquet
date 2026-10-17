using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Bonsai.Apache.Common
{
    internal static class WriterPipeline
    {
        internal static IObservable<T> Process<T>(IObservable<T> source, bool buffered, int queueCapacity, TimeSpan flushInterval,
            Func<WriterSink<T>> createSink, Func<T, T>? retain = null, Action<T>? release = null)
        {
            if (!buffered)
                return Observable.Using(createSink, sink => source.Do(sink.Write)
                    .Do(_ => { }, _ => sink.Dispose(), sink.Dispose));

            return Observable.Create<T>(observer =>
            {
                var session = new Session<T>(observer, queueCapacity, flushInterval, createSink, retain, release);
                Task.Factory.StartNew(session.Run, CancellationToken.None,
                    TaskCreationOptions.LongRunning, TaskScheduler.Default);
                session.Upstream.Disposable = source.Subscribe(session);
                return session;
            });
        }

        sealed class Session<T> : IObserver<T>, IDisposable
        {
            readonly BlockingCollection<Request> queue;
            readonly IObserver<T> observer;
            readonly TimeSpan flushInterval;
            readonly Func<WriterSink<T>> createSink;
            readonly Func<T, T>? retain;
            readonly Action<T>? release;
            Exception? sourceError;
            int stopped, cancelled;
            internal SingleAssignmentDisposable Upstream { get; } = new SingleAssignmentDisposable();

            readonly struct Request
            {
                internal readonly T Value, Output;
                internal Request(T value, T output) { Value = value; Output = output; }
            }

            internal Session(IObserver<T> observer, int queueCapacity, TimeSpan flushInterval, Func<WriterSink<T>> createSink,
                Func<T, T>? retain, Action<T>? release)
            {
                this.observer = observer;
                this.flushInterval = flushInterval;
                this.createSink = createSink;
                this.retain = retain;
                this.release = release;
                queue = new BlockingCollection<Request>(queueCapacity);
            }

            public void OnNext(T value)
            {
                if (Volatile.Read(ref stopped) != 0) return;
                try
                {
                    var item = retain == null ? value : retain(value);
                    bool accepted = false;
                    try
                    {
                        accepted = queue.TryAdd(new Request(item, value));
                        if (!accepted) OnError(new InvalidOperationException("The background writer queue is full."));
                    }
                    catch (InvalidOperationException) when (Volatile.Read(ref stopped) != 0) { }
                    finally { if (!accepted) release?.Invoke(item); }
                }
                catch (Exception ex) { OnError(ex); }
            }

            public void OnError(Exception error) { Stop(error); Upstream.Dispose(); }
            public void OnCompleted() => Stop(null);

            void Stop(Exception? error)
            {
                if (Interlocked.Exchange(ref stopped, 1) != 0) return;
                sourceError = error;
                queue.CompleteAdding();
            }

            internal void Run()
            {
                Exception? failure = null;
                try
                {
                    // This worker alone opens, writes, flushes, and closes the file.
                    using var sink = createSink();
                    var clock = Stopwatch.StartNew();
                    var nextFlush = flushInterval;
                    while (!queue.IsCompleted)
                    {
                        var wait = flushInterval == TimeSpan.Zero ? Timeout.Infinite :
                            (int)Math.Min(int.MaxValue, Math.Max(0, Math.Ceiling((nextFlush - clock.Elapsed).TotalMilliseconds)));
                        if (queue.TryTake(out var request, wait))
                        {
                            try
                            {
                                sink.Write(request.Value);
                                if (Volatile.Read(ref cancelled) == 0) observer.OnNext(request.Output);
                            }
                            finally { release?.Invoke(request.Value); }
                        }
                        if (flushInterval > TimeSpan.Zero && clock.Elapsed >= nextFlush)
                        {
                            sink.Flush();
                            nextFlush = clock.Elapsed + flushInterval;
                        }
                    }
                }
                catch (Exception ex) { failure = ex; Stop(ex); }
                finally
                {
                    Upstream.Dispose();
                    while (queue.TryTake(out var pending)) release?.Invoke(pending.Value);
                    queue.Dispose();
                }
                if (Volatile.Read(ref cancelled) == 0)
                {
                    var error = failure ?? sourceError;
                    if (error == null) observer.OnCompleted();
                    else observer.OnError(error);
                }
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref cancelled, 1);
                Stop(null);
                Upstream.Dispose();
                // No worker join: shutdown must not wait for disk I/O on the caller.
            }
        }
    }
}
