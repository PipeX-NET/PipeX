using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;

namespace PipeX.Transport;

/// <summary>
/// Accept-loop for PipeX servers. Each incoming connection is handed to a dedicated
/// <see cref="Task"/> scheduled by the ThreadPool (work-stealing) — there is no
/// <c>EventLoop</c> pinning a connection to a fixed thread.
/// </summary>
/// <remarks>
/// <para>The listener binds to an <see cref="EndPoint"/>, accepts connections in a loop and
/// invokes <c>onConnected</c> for each accepted socket. The handler is awaited on a
/// background <see cref="Task"/> (fire-and-forget from the accept loop's point of view), so
/// the next connection is accepted without waiting for the previous handler to finish.</para>
/// <para>Exceptions thrown from <c>onConnected</c> are captured and logged via
/// <see cref="Console.Error"/>; they do not bring down the accept loop.</para>
/// <para>Calling <see cref="StopAsync"/> (or cancelling the <c>cancellationToken</c> passed to
/// <see cref="RunAsync"/>) makes the accept loop exit cleanly. In-flight handlers are
/// signalled via the same token.</para>
/// </remarks>
public sealed class PipeXListener
{
    private readonly EndPoint _endpoint;
    private readonly Func<IDuplexPipe, CancellationToken, Task> _onConnected;
    private readonly PipeOptions? _pipeOptions;
    private readonly List<Task> _activeHandlers = new();
    private readonly object _activeHandlersLock = new();

    /// <summary>
    /// Creates a listener that will bind to <paramref name="endpoint"/> and dispatch each
    /// accepted connection to <paramref name="onConnected"/>.
    /// </summary>
    /// <param name="endpoint">Local endpoint to bind the listening socket to.</param>
    /// <param name="onConnected">Handler invoked once per accepted connection.</param>
    /// <param name="pipeOptions">Optional pipe options passed to <see cref="SocketDuplexPipe"/>.</param>
    public PipeXListener(
        EndPoint endpoint,
        Func<IDuplexPipe, CancellationToken, Task> onConnected,
        PipeOptions? pipeOptions = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(onConnected);
        _endpoint = endpoint;
        _onConnected = onConnected;
        _pipeOptions = pipeOptions;
    }

    /// <summary>
    /// Runs the accept loop until <paramref name="ct"/> is cancelled. Returns the
    /// <see cref="Task"/> representing the accept loop itself; the per-connection handlers run
    /// concurrently as separate <see cref="Task"/>s.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        using var listenSocket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        listenSocket.Bind(_endpoint);
        listenSocket.Listen();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                Socket socket;
                try
                {
                    socket = await listenSocket.AcceptAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    // Listener was stopped; the socket was disposed under us.
                    break;
                }

                // Fire-and-forget: do NOT await here or the next connection has to wait for the
                // previous one to finish processing.
                _ = HandleAsync(socket, ct);
            }
        }
        finally
        {
            // Drain in-flight handlers before returning to the caller.
            await DrainAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tracked handlers as a snapshot — useful for tests asserting that no handler is leaked.
    /// </summary>
    internal IReadOnlyList<Task> ActiveHandlersSnapshot()
    {
        lock (_activeHandlersLock)
        {
            return _activeHandlers.ToArray();
        }
    }

    private async Task HandleAsync(Socket socket, CancellationToken ct)
    {
        var registration = RegisterHandler();
        try
        {
            await using var pipe = new SocketDuplexPipe(socket, _pipeOptions);
            await _onConnected(pipe, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (Exception ex)
        {
            // An exception in a connection handler MUST NOT bring down the accept loop.
            Console.Error.WriteLine($"[PipeXListener] connection handler faulted: {ex}");
        }
        finally
        {
            try { socket.Shutdown(SocketShutdown.Both); } catch { /* socket may already be gone */ }
            socket.Dispose();
            registration.Dispose();
        }
    }

    private HandlerRegistration RegisterHandler()
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_activeHandlersLock)
        {
            _activeHandlers.Add(tcs.Task);
        }
        return new HandlerRegistration(this, tcs);
    }

    private void UnregisterHandler(Task task)
    {
        lock (_activeHandlersLock)
        {
            _activeHandlers.Remove(task);
        }
    }

    private async Task DrainAsync()
    {
        Task[] snapshot;
        lock (_activeHandlersLock)
        {
            snapshot = _activeHandlers.ToArray();
        }
        try { await Task.WhenAll(snapshot).ConfigureAwait(false); }
        catch { /* individual handler exceptions are swallowed at their source */ }
    }

    private readonly struct HandlerRegistration : IDisposable
    {
        private readonly PipeXListener _owner;
        private readonly TaskCompletionSource _tcs;

        public HandlerRegistration(PipeXListener owner, TaskCompletionSource tcs)
        {
            _owner = owner;
            _tcs = tcs;
        }

        public void Dispose()
        {
            _tcs.TrySetResult();
            _owner.UnregisterHandler(_tcs.Task);
        }
    }
}
