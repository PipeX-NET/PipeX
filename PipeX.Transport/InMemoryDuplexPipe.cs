using System.IO.Pipelines;

namespace PipeX.Transport;

/// <summary>
/// An in-process <see cref="IDuplexPipe"/> pair where bytes written to one side's
/// <see cref="Output"/> are visible on the other side's <see cref="Input"/> (and vice versa).
/// Used by tests and by callers that want a fully in-memory transport without a real socket.
/// </summary>
/// <remarks>
/// <para>
/// The pair is created with <see cref="CreatePair(System.IO.Pipelines.PipeOptions?)"/>. Both
/// halves share the same <see cref="PipeOptions"/> so backpressure behaviour mirrors the
/// production transport.
/// </para>
/// <para>
/// Lifetime: when one side <c>Completes</c> its <see cref="Output"/>, the other side's
/// <see cref="Input"/> will observe <c>IsCompleted == true</c> on its next read. The reverse is
/// also true. This matches the contract of <c>SocketDuplexPipe</c>.
/// </para>
/// </remarks>
public sealed class InMemoryDuplexPipe : IDuplexPipe
{
    private readonly PipeReader _input;
    private readonly PipeWriter _output;

    internal InMemoryDuplexPipe(PipeReader input, PipeWriter output)
    {
        _input = input;
        _output = output;
    }

    /// <inheritdoc />
    public PipeReader Input => _input;

    /// <inheritdoc />
    public PipeWriter Output => _output;

    /// <summary>
    /// Create a connected pair of in-memory duplex pipes. Anything written to <c>a.Output</c>
    /// appears on <c>b.Input</c> and vice versa.
    /// </summary>
    public static (InMemoryDuplexPipe A, InMemoryDuplexPipe B) CreatePair(PipeOptions? options = null)
    {
        options ??= PipeXPipeOptions.Default;

        // Two pipes: pipe1 carries a->b, pipe2 carries b->a.
        var aToB = new Pipe(options);
        var bToA = new Pipe(options);

        var a = new InMemoryDuplexPipe(input: bToA.Reader, output: aToB.Writer);
        var b = new InMemoryDuplexPipe(input: aToB.Reader, output: bToA.Writer);
        return (a, b);
    }
}
