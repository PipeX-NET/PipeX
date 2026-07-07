using System.IO.Pipelines;

namespace PipeX.Transport;

/// <summary>
/// A full-duplex byte pipe that hides the underlying transport (e.g. <see cref="System.Net.Sockets.Socket"/>)
/// behind a pair of <see cref="PipeReader"/>/<see cref="PipeWriter"/>. Layer 3 (Codec) and Layer 4
/// (Protocol) only ever see this interface; they never touch <c>Socket</c> directly.
/// </summary>
public interface IDuplexPipe
{
    /// <summary>Reads bytes that arrived from the peer.</summary>
    PipeReader Input { get; }

    /// <summary>Writes bytes that should be sent to the peer.</summary>
    PipeWriter Output { get; }
}
