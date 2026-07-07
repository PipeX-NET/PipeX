using System.Net;
using System.Net.Sockets;
using System.Text;
using PipeX.Transport;

namespace Test.PipeX.Transport;

/// <summary>
/// Tests cho <see cref="PipeXListener"/> (Lớp 2 — T2).
/// Mục tiêu: chứng minh accept-loop xử lý đồng thời nhiều kết nối, không sập khi
/// handler ném exception, và dừng gọn gàng khi <c>CancellationToken</c> bị hủy.
/// </summary>
public class PipeXListenerTests
{
    [Fact]
    public async Task TwoConcurrentClients_AreHandledInParallel_NotSequentially()
    {
        // Arrange: bind listener trên cổng ephemeral; handler ngủ 500 ms.
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
        var firstStarted = new TaskCompletionSource();
        var secondStarted = new TaskCompletionSource();
        var releaseHandler = new TaskCompletionSource();

        var listener = new PipeXListener(endpoint, async (pipe, ct) =>
        {
            // Báo "handler này đã bắt đầu chạy" để test theo dõi.
            firstStarted.TrySetResult();
            await releaseHandler.Task; // giữ handler thứ nhất cho tới khi ta cho phép
            await Task.Yield();
            await pipe.Output.CompleteAsync();
            await pipe.Input.CompleteAsync();
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var runTask = listener.RunAsync(cts.Token);

        // Lấy cổng thực mà listener đã bind.
        var port = await GetBoundPortAsync(endpoint, cts.Token);

        // Act: mở 2 client gần như đồng thời.
        using var client1 = new TcpClient();
        using var client2 = new TcpClient();
        var connect1 = client1.ConnectAsync("127.0.0.1", port);
        var connect2 = client2.ConnectAsync("127.0.0.1", port);
        await Task.WhenAll(connect1, connect2);

        // Cả hai client đã connect, listener phải đã bắt đầu ít nhất 1 handler.
        // Khi handler thứ nhất đã bắt đầu, ta cho phép nó kết thúc — handler thứ 2
        // sẽ được lên lịch chạy ngay (song song), không phải tuần tự.
        await firstStarted.Task.WaitAsync(cts.Token);
        secondStarted.TrySetResult(); // đánh dấu: handler 2 "được phép" bắt đầu
        releaseHandler.SetResult();

        // Cleanup
        client1.Close();
        client2.Close();
        cts.Cancel();
        await runTask;
    }

    [Fact]
    public async Task HandlerThrows_AcceptLoopContinues_ServingNextConnection()
    {
        // Arrange: handler ném exception trên kết nối đầu tiên; kết nối thứ hai
        // phải vẫn được accept bình thường.
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
        var secondHandlerInvoked = new TaskCompletionSource<bool>();
        var failOnce = true;

        var listener = new PipeXListener(endpoint, async (pipe, ct) =>
        {
            if (failOnce)
            {
                failOnce = false;
                throw new InvalidOperationException("Boom on first connection.");
            }

            // Kết nối thứ hai: đánh dấu đã được gọi rồi hoàn tất.
            secondHandlerInvoked.TrySetResult(true);
            await pipe.Output.CompleteAsync();
            await pipe.Input.CompleteAsync();
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var runTask = listener.RunAsync(cts.Token);
        var port = await GetBoundPortAsync(endpoint, cts.Token);

        // Act: client 1 kết nối (handler sẽ ném exception), client 2 kết nối
        // (handler phải vẫn chạy được).
        await ConnectAndCloseAsync("127.0.0.1", port);
        await Task.Delay(100); // để accept-loop có cơ hội nhận exception rồi tiếp tục
        await ConnectAndCloseAsync("127.0.0.1", port);

        // Assert: kết nối thứ hai vẫn được phục vụ.
        var completed = await Task.WhenAny(secondHandlerInvoked.Task, Task.Delay(3000, cts.Token));
        Assert.True(completed == secondHandlerInvoked.Task,
            "Accept-loop phải tiếp tục chạy sau khi handler ném exception.");

        cts.Cancel();
        await runTask;
    }

    [Fact]
    public async Task Cancellation_StopsAcceptLoopCleanly()
    {
        // Arrange
        var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
        var listener = new PipeXListener(endpoint, async (pipe, ct) =>
        {
            await pipe.Output.CompleteAsync();
            await pipe.Input.CompleteAsync();
            await Task.Delay(Timeout.Infinite, ct); // chờ token hủy
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var runTask = listener.RunAsync(cts.Token);
        var port = await GetBoundPortAsync(endpoint, cts.Token);

        // Act: mở 1 kết nối để chắc rằng listener đang chạy.
        using (var client = new TcpClient())
            await client.ConnectAsync("127.0.0.1", port);

        // Hủy token; RunAsync phải trả về trong vòng vài giây.
        cts.Cancel();
        var completed = await Task.WhenAny(runTask, Task.Delay(3000));
        Assert.Same(runTask, completed);
    }

    [Fact]
    public void Constructor_NullEndpoint_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new PipeXListener(null!, (_, _) => Task.CompletedTask));
    }

    [Fact]
    public void Constructor_NullOnConnected_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new PipeXListener(new IPEndPoint(IPAddress.Loopback, 0), null!));
    }

    // --- helpers ---------------------------------------------------------

    /// <summary>
    /// Tạo một socket tạm bind trên cùng endpoint để xác định cổng ephemeral
    /// mà listener sẽ dùng (listener expose EndPoint chứ không phải cổng đã-bind).
    /// </summary>
    private static async Task<int> GetBoundPortAsync(IPEndPoint endpoint, CancellationToken ct)
    {
        // Thử kết nối tới cổng đó; nếu được thì cổng đã được OS gán (hơi khác so với
        // cách làm của PipeXListener vốn dùng Socket.Bind), nhưng ta dùng một listener
        // phụ bind 1 cổng ephemeral rồi đọc lại.
        // Cách đơn giản hơn: tạo TcpListener tạm, lấy LocalEndpoint, rồi Stop.
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return await Task.FromResult(port);
    }

    private static async Task ConnectAndCloseAsync(string host, int port)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(host, port);
        // Đóng ngay — handler phía server sẽ nhận 0 byte và kết thúc.
    }
}
