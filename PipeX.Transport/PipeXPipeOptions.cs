using System.Buffers;
using System.IO.Pipelines;

namespace PipeX.Transport;

/// <summary>
/// Centralised <see cref="PipeOptions"/> for the PipeX transport. The thresholds control the
/// backpressure watermark of every internal pipe: writers pause when unread bytes exceed
/// <see cref="PipeOptions.PauseWriterThreshold"/> and resume once they fall below
/// <see cref="PipeOptions.ResumeWriterThreshold"/>.
/// </summary>
public static class PipeXPipeOptions
{
    /// <summary>Default high-water mark (1 MiB) — equivalent to Netty's <c>highWaterMark</c>.</summary>
    public const int DefaultPauseWriterThreshold = 1 << 20;  // 1 MiB

    /// <summary>Default low-water mark (512 KiB). MUST be smaller than the pause threshold.</summary>
    public const int DefaultResumeWriterThreshold = 1 << 19; // 512 KiB

    /// <summary>
    /// Sensible defaults for PipeX pipes: pool from <see cref="MemoryPool{T}.Shared"/> and apply a
    /// 1 MiB / 512 KiB backpressure window. Override individual fields if you need a different
    /// strategy.
    /// </summary>
    public static readonly PipeOptions Default = new(
        pool: MemoryPool<byte>.Shared,
        pauseWriterThreshold: DefaultPauseWriterThreshold,
        resumeWriterThreshold: DefaultResumeWriterThreshold);

    /// <summary>
    /// Build a <see cref="PipeOptions"/> for a one-shot test scenario with a tiny backpressure
    /// window. Useful in unit tests where we want <c>FlushAsync</c> to back-pressure with only a
    /// few hundred bytes of buffered data.
    /// </summary>
    public static PipeOptions ForTest(int pauseWriterThreshold, int resumeWriterThreshold) =>
        new(
            pool: MemoryPool<byte>.Shared,
            pauseWriterThreshold: pauseWriterThreshold,
            resumeWriterThreshold: resumeWriterThreshold);
}
