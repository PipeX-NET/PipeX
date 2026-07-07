using System.IO.Pipelines;
using System.Net.Sockets;

namespace PipeX.Transport;

/// <summary>
/// Production <see cref="IDuplexPipe"/> implementation backed by a connected
/// <see cref="Socket"/>. Hides the socket behind a pair of <see cref="PipeReader"/>/
/// <see cref="PipeWriter"/> so the Codec/Protocol layers never touch <c>Socket</c> directly.
/// </summary>
/// <remarks>
/// <para>Read direction: a background pump calls <c>Socket.ReceiveAsync</c> and forwards the
/// bytes into the internal <c>_readPipe</c>. The codec reads from <see cref="Input"/>.</para>
/// <para>Write direction: the codec writes to <see cref="Output"/>, which is the writer of the
/// internal <c>_writePipe</c>. A second background pump drains that pipe and calls
/// <c>Socket.SendAsync</c>. Backpressure is applied automatically by
/// <see cref="PipeOptions.PauseWriterThreshold"/>.</para>
/// <para>Lifetime: <see cref="DisposeAsync"/> cancels both pumps, completes both pipes and
/// disposes the socket. The codec will then see <c>IsCompleted == true</c> on its next read.</para>
/// </remarks>
public sealed class SocketDuplexPipe : IDuplexPipe, IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly Pipe _readPipe;
    private readonly Pipe _writePipe;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _readPump;
    private readonly Task _writePump;
    private int _disposed;

    /// <inheritdoc />
    public PipeReader Input => _readPipe.Reader;

    /// <inheritdoc />
    public PipeWriter Output => _writePipe.Writer;

    /// <summary>
    /// Build a duplex pipe around an already-connected <paramref name="socket"/>. The socket is
    /// owned by this instance and will be disposed by <see cref="DisposeAsync"/>.
    /// </summary>
    /// <param name="socket">Connected TCP socket (server-accepted or client-connected).</param>
    /// <param name="options">Optional pipe options. Defaults to <see cref="PipeXPipeOptions.Default"/>.</param>
    public SocketDuplexPipe(Socket socket, PipeOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(socket);
        if (socket.SocketType != SocketType.Stream || socket.ProtocolType != ProtocolType.Tcp)
            throw new ArgumentException("Socket must be a connected TCP stream socket.", nameof(socket));

        _socket = socket;
        _readPipe = new Pipe(options ?? PipeXPipeOptions.Default);
        _writePipe = new Pipe(options ?? PipeXPipeOptions.Default);

        _readPump = Task.Run(ReadPumpAsync);
        _writePump = Task.Run(WritePumpAsync);
    }

    private async Task ReadPumpAsync()
    {
        Exception? failure = null;
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var memory = _readPipe.Writer.GetMemory(4096);
                int received;
                try
                {
                    received = await _socket.ReceiveAsync(memory, SocketFlags.None, _cts.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }
                catch (SocketException ex) { failure = ex; break; }

                if (received == 0) break; // Peer closed the socket cleanly.

                _readPipe.Writer.Advance(received);
                var flush = await _readPipe.Writer.FlushAsync().ConfigureAwait(false);
                if (flush.IsCompleted || flush.IsCanceled) break;
            }
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            _readPipe.Writer.Complete(failure);
        }
    }

    private async Task WritePumpAsync()
    {
        Exception? failure = null;
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var read = await _writePipe.Reader.ReadAsync(_cts.Token).ConfigureAwait(false);
                var buffer = read.Buffer;

                if (!buffer.IsEmpty)
                {
                    try
                    {
                        foreach (var segment in buffer)
                        {
                            await _socket.SendAsync(segment, SocketFlags.None, _cts.Token)
                                .ConfigureAwait(false);
                        }
                    }
                    catch (OperationCanceledException) { break; }
                    catch (SocketException ex) { failure = ex; break; }
                }

                _writePipe.Reader.AdvanceTo(buffer.End);

                if (read.IsCompleted) break;
            }
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            _writePipe.Reader.Complete();
            _writePipe.Writer.Complete(failure);
        }
    }

    /// <summary>
    /// Stops both pumps, completes both pipes and disposes the underlying socket. Safe to call
    /// more than once.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _cts.Cancel();

        // Best-effort graceful shutdown of the socket; the pumps are exiting anyway.
        try { _socket.Shutdown(SocketShutdown.Both); } catch { /* socket already gone */ }
        _socket.Dispose();

        // The pumps complete their respective pipe ends in their finally blocks once the
        // cancellation token trips. Just wait for them to drain.
        try { await _readPump.ConfigureAwait(false); } catch { /* swallow */ }
        try { await _writePump.ConfigureAwait(false); } catch { /* swallow */ }
        
        _cts.Dispose();
    }
}
