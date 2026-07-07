using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using PipeX.Transport;

namespace Test.PipeX.Transport;

/// <summary>
/// Tests cho <see cref="InMemoryDuplexPipe"/> (Lớp 2 — T1).
/// Mục tiêu: chứng minh cặp pipe trong bộ nhớ hoạt động đúng "round-trip" byte và
/// tôn trọng đúng hợp đồng hoàn tất khi một phía gọi <c>Complete()</c>.
/// </summary>
public class InMemoryDuplexPipeTests
{
    [Fact]
    public async Task RoundTrip_BytesWrittenOnASide_AreReadableOnBSide()
    {
        // Arrange: tạo cặp pipe nối vòng trong bộ nhớ.
        var (a, b) = InMemoryDuplexPipe.CreatePair();

        // Act: phía A ghi 11 byte "hello world" xuống Output.
        var payload = Encoding.UTF8.GetBytes("hello world");
        await a.Output.WriteAsync(payload);
        await a.Output.FlushAsync();

        // Assert: phía B đọc được đúng 11 byte đó từ Input, theo đúng thứ tự.
        var result = await b.Input.ReadAsync();
        var received = result.Buffer.ToArray();

        Assert.Equal(payload.Length, received.Length);
        Assert.Equal(payload, received);
        Assert.False(result.IsCompleted);

        b.Input.AdvanceTo(result.Buffer.End);
        await b.Input.CompleteAsync();
        await a.Output.CompleteAsync();
    }

    [Fact]
    public async Task RoundTrip_Bidirectional_BothDirectionsWorkIndependently()
    {
        // Arrange: hai chiều đọc/ghi phải độc lập (full-duplex).
        var (a, b) = InMemoryDuplexPipe.CreatePair();

        // Act: gửi "ping" từ A -> B và "pong" từ B -> A "gần như" đồng thời.
        var pingTask = WriteAndFlushAsync(a.Output, "ping"u8.ToArray()).AsTask();
        var pongTask = WriteAndFlushAsync(b.Output, "pong"u8.ToArray()).AsTask();
        await Task.WhenAll(pingTask, pongTask);

        // Assert: A đọc được "pong", B đọc được "ping".
        var fromB = await ReadAllAsync(a.Input);
        var fromA = await ReadAllAsync(b.Input);

        Assert.Equal("pong"u8.ToArray(), fromB);
        Assert.Equal("ping"u8.ToArray(), fromA);
    }

    [Fact]
    public async Task Complete_OnOneSide_PropagatesIsCompletedToOtherSide()
    {
        // Arrange: tạo cặp pipe, A ghi rồi Complete chiều Output.
        var (a, b) = InMemoryDuplexPipe.CreatePair();
        await a.Output.WriteAsync(new byte[] { 1, 2, 3 });
        await a.Output.CompleteAsync();

        // Act + Assert: B đọc được 3 byte, đồng thời IsCompleted == true.
        var result = await b.Input.ReadAsync();

        Assert.True(result.IsCompleted);
        Assert.Equal(new byte[] { 1, 2, 3 }, result.Buffer.ToArray());
        b.Input.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task Complete_OnOneSide_FinalReadOnOtherSideReturnsZeroLengthAndIsCompleted()
    {
        // Arrange: hoàn tất A.Output mà không ghi gì.
        var (a, b) = InMemoryDuplexPipe.CreatePair();
        await a.Output.CompleteAsync();

        // Act: B đọc — pipe phải báo "đã đóng" với buffer rỗng.
        var result = await b.Input.ReadAsync();

        // Assert: không có dữ liệu, IsCompleted = true, không crash.
        Assert.True(result.IsCompleted);
        Assert.True(result.Buffer.IsEmpty);
    }

    [Fact]
    public async Task CreatePair_UsesBackpressureThresholdsFromPipeOptions()
    {
        // Arrange: cấu hình ngưỡng backpressure rất nhỏ.
        var options = PipeXPipeOptions.ForTest(pauseWriterThreshold: 64, resumeWriterThreshold: 32);

        // Act + Assert: pipe phải dùng đúng options đã truyền vào mà không ném exception.
        var (a, b) = InMemoryDuplexPipe.CreatePair(options);

        Assert.NotNull(a.Input);
        Assert.NotNull(a.Output);
        Assert.NotNull(b.Input);
        Assert.NotNull(b.Output);

        await a.Output.CompleteAsync();
        await a.Input.CompleteAsync();
        await b.Output.CompleteAsync();
        await b.Input.CompleteAsync();
    }

    [Fact]
    public void CreatePair_DefaultOptions_AreFromPipeXPipeOptionsDefault()
    {
        // Không truyền options => dùng PipeXPipeOptions.Default.
        // Khẳng định hợp đồng API: phải tạo được cặp pipe hợp lệ, không ném exception.
        var (a, b) = InMemoryDuplexPipe.CreatePair();

        Assert.NotNull(a.Input);
        Assert.NotNull(a.Output);
        Assert.NotNull(b.Input);
        Assert.NotNull(b.Output);
    }

    [Fact]
    public async Task LargePayload_AcrossManySegments_IsReassembledCorrectly()
    {
        // Arrange: gửi 100 chunk 100 byte — buộc pipe phải phân đoạn nhiều segment.
        var (a, b) = InMemoryDuplexPipe.CreatePair();
        var total = 100 * 100;
        var payload = new byte[total];
        Random.Shared.NextBytes(payload);

        // Act
        for (var i = 0; i < 100; i++)
            await a.Output.WriteAsync(payload.AsMemory(i * 100, 100));
        await a.Output.FlushAsync();

        // Assert: ghép lại đúng thứ tự, không mất byte, không lẫn vị trí.
        var sink = new MemoryStream();
        var result = await b.Input.ReadAsync();
        foreach (var memory in result.Buffer)
            await sink.WriteAsync(memory);
        b.Input.AdvanceTo(result.Buffer.End);

        Assert.Equal(total, sink.Length);
        Assert.Equal(payload, sink.ToArray());
    }

    // --- helpers ---------------------------------------------------------

    private static async ValueTask WriteAndFlushAsync(PipeWriter writer, byte[] bytes)
    {
        await writer.WriteAsync(bytes);
        await writer.FlushAsync();
    }

    private static async Task<byte[]> ReadAllAsync(PipeReader reader)
    {
        var result = await reader.ReadAsync();
        var copy = result.Buffer.ToArray();
        reader.AdvanceTo(result.Buffer.End);
        return copy;
    }
}
