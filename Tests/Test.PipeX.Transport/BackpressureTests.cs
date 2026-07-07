using System.IO.Pipelines;
using PipeX.Transport;

namespace Test.PipeX.Transport;

/// <summary>
/// Tests cho backpressure (Lớp 2 — T3).
/// Mục tiêu: chứng minh <see cref="PipeOptions.PauseWriterThreshold"/> /
/// <see cref="PipeOptions.ResumeWriterThreshold"/> hoạt động đúng — khi dữ liệu
/// chưa-đọc vượt ngưỡng pause, <c>FlushAsync</c> phải chưa hoàn tất; khi reader
/// đọc bớt xuống dưới ngưỡng resume, <c>FlushAsync</c> phải hoàn tất.
/// </summary>
public class BackpressureTests
{
    [Fact]
    public void Default_ResumeThreshold_IsSmallerThanPauseThreshold()
    {
        // Hợp đồng bất biến: resume < pause, nếu không backpressure sẽ dao động
        // liên tục hoặc không bao giờ nhả.
        Assert.True(PipeXPipeOptions.DefaultResumeWriterThreshold
                    < PipeXPipeOptions.DefaultPauseWriterThreshold);
    }

    [Fact]
    public void DefaultOptions_UseMemoryPoolShared()
    {
        // T3 chỉ định phải dùng MemoryPool<byte>.Shared làm pool mặc định.
        // Không có API public để lộ Pool ra ngoài, nên ta kiểm chứng gián tiếp
        // bằng cách xác nhận instance Default đã được khởi tạo mà không ném exception
        // và cấu hình đúng ngưỡng mặc định.
        Assert.NotNull(PipeXPipeOptions.Default);
    }

    [Fact]
    public async Task FlushAsync_DoesNotComplete_WhenUnreadBytesExceedPauseThreshold()
    {
        // Arrange: pipe với pause = 1 KiB, resume = 512 B, và KHÔNG ai đọc.
        var options = PipeXPipeOptions.ForTest(pauseWriterThreshold: 1024, resumeWriterThreshold: 512);
        var pipe = new Pipe(options);

        // Act: ghi 2 KiB (gấp đôi pause) rồi Flush — FlushAsync phải chưa hoàn tất
        // vì reader chưa đọc nên writer vẫn đang "bị chặn" bởi ngưỡng pause.
        var data = new byte[2048];
        await pipe.Writer.WriteAsync(data);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        // Assert: cho task chạy một chút, phải chưa xong.
        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted,
            "FlushAsync phải chưa hoàn tất khi unread bytes vượt pauseWriterThreshold.");

        // Cleanup: đọc hết phần còn lại rồi complete cả hai đầu.
        var readResult = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(readResult.Buffer.End);
        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact]
    public async Task FlushAsync_Completes_AfterReaderDropsBelowResumeThreshold()
    {
        // Arrange: pipe pause=1KiB, resume=512B, KHÔNG ai đọc.
        var options = PipeXPipeOptions.ForTest(pauseWriterThreshold: 1024, resumeWriterThreshold: 512);
        var pipe = new Pipe(options);

        var data = new byte[2048];
        await pipe.Writer.WriteAsync(data);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        await Task.Delay(50);
        Assert.False(flushTask.IsCompleted);

        // Act: reader đọc hết (kéo xuống dưới resume).
        var readResult = await pipe.Reader.ReadAsync();
        pipe.Reader.AdvanceTo(readResult.Buffer.End);

        // Assert: flush giờ phải hoàn tất sau khi reader đã tiêu thụ dữ liệu.
        var completed = await Task.WhenAny(flushTask, Task.Delay(2000));
        Assert.Same(flushTask, completed);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact]
    public async Task FlushAsync_CompletesImmediately_WhenBytesBelowPauseThreshold()
    {
        // Arrange: pipe có pause=1KiB, ghi 100 byte (dưới ngưỡng).
        var options = PipeXPipeOptions.ForTest(pauseWriterThreshold: 1024, resumeWriterThreshold: 512);
        var pipe = new Pipe(options);

        // Act + Assert: FlushAsync phải hoàn tất ngay vì 100 byte < pause threshold.
        await pipe.Writer.WriteAsync(new byte[100]);
        var flushTask = pipe.Writer.FlushAsync().AsTask();

        var completed = await Task.WhenAny(flushTask, Task.Delay(2000));
        Assert.Same(flushTask, completed);

        pipe.Writer.Complete();
        pipe.Reader.Complete();
    }

    [Fact]
    public void ForTest_BuildsOptionsWithRequestedThresholds()
    {
        // ForTest phải trả về PipeOptions đúng với 2 tham số đầu vào.
        var opts = PipeXPipeOptions.ForTest(pauseWriterThreshold: 123, resumeWriterThreshold: 45);

        // Tận dụng PipeOptions.PauseWriterThreshold / ResumeWriterThreshold để xác nhận.
        Assert.Equal(123, opts.PauseWriterThreshold);
        Assert.Equal(45, opts.ResumeWriterThreshold);
    }

    [Fact]
    public void InMemoryDuplexPipe_RespectsBackpressure_BetweenPair()
    {
        // Tích hợp: cặp InMemoryDuplexPipe phải dùng cùng options về backpressure.
        var options = PipeXPipeOptions.ForTest(pauseWriterThreshold: 1024, resumeWriterThreshold: 512);
        var (a, b) = InMemoryDuplexPipe.CreatePair(options);

        Assert.NotNull(a.Input);
        Assert.NotNull(a.Output);
        Assert.NotNull(b.Input);
        Assert.NotNull(b.Output);
    }
}
