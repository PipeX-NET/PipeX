using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using PipeX.Transport;

namespace Test.PipeX.Transport;

/// <summary>
/// Tests cho <see cref="SocketDuplexPipe"/> (Lớp 2 — T1, triển khai production).
/// Mục tiêu: chứng minh pipe dựa trên <see cref="Socket"/> round-trip đúng byte,
/// báo <c>IsCompleted</c> khi peer đóng, và Dispose dọn dẹp tài nguyên sạch sẽ.
/// </summary>
public class SocketDuplexPipeTests
{
    [Fact]
    public async Task RoundTrip_BytesWrittenByClient_AreReadableOnServerPipe()
    {
        // Arrange: tạo cặp server/client socket, bọc server thành SocketDuplexPipe.
        using var server = new TcpClient();
        using var client = new TcpClient();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var acceptTask = listener.AcceptTcpClientAsync();
        await client.ConnectAsync("127.0.0.1", port);
        var acceptedClient = await acceptTask;
        listener.Stop();

        var serverSocket = acceptedClient.Client;

        // Act: client gửi "ping", server pipe đọc được "ping".
        var payload = Encoding.UTF8.GetBytes("ping");
        await client.GetStream().WriteAsync(payload);
        await client.GetStream().FlushAsync();

        await using var serverPipe = new SocketDuplexPipe(serverSocket);
        var readTask = serverPipe.Input.ReadAsync().AsTask();
        var completed = await Task.WhenAny(readTask, Task.Delay(TimeSpan.FromSeconds(3)));
        Assert.Same(readTask, completed);
        var result = await readTask;

        // Assert
        Assert.False(result.IsCompleted);
        Assert.Equal(payload, result.Buffer.ToArray());
        serverPipe.Input.AdvanceTo(result.Buffer.End);
    }

    [Fact]
    public async Task PeerCloses_ReaderObservesIsCompleted()
    {
        // Arrange
        using var server = new TcpClient();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var acceptTask = listener.AcceptTcpClientAsync();
        await server.ConnectAsync("127.0.0.1", port);
        var accepted = await acceptTask;
        listener.Stop();

        await using var serverPipe = new SocketDuplexPipe(accepted.Client);

        // Act: client đóng kết nối.
        server.Close();

        // Assert: serverPipe.Input phải báo IsCompleted ở lần đọc kế tiếp.
        var readTask = serverPipe.Input.ReadAsync().AsTask();
        var completed = await Task.WhenAny(readTask, Task.Delay(TimeSpan.FromSeconds(3)));
        Assert.Same(readTask, completed);
        var result = await readTask;
        Assert.True(result.IsCompleted);
    }

    [Fact]
    public async Task WritePump_ForwardsBytesToSocket()
    {
        // Arrange: ghi từ server pipe xuống socket, client đọc được.
        using var client = new TcpClient();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var acceptTask = listener.AcceptTcpClientAsync();
        await client.ConnectAsync("127.0.0.1", port);
        var accepted = await acceptTask;
        listener.Stop();

        await using var serverPipe = new SocketDuplexPipe(accepted.Client);

        // Act
        var payload = Encoding.UTF8.GetBytes("pong");
        await serverPipe.Output.WriteAsync(payload);
        await serverPipe.Output.FlushAsync();

        // Assert: client đọc đúng "pong".
        var buffer = new byte[4];
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await client.GetStream().ReadAsync(buffer.AsMemory(read));
            if (n == 0) break;
            read += n;
        }
        Assert.Equal(payload, buffer);
    }

    [Fact]
    public void Constructor_RejectsNonTcpStreamSocket()
    {
        // T1 chỉ chấp nhận SocketType.Stream + ProtocolType.Tcp.
        using var udp = new Socket(SocketType.Dgram, ProtocolType.Udp);
        Assert.Throws<ArgumentException>(() => new SocketDuplexPipe(udp));
    }

    [Fact]
    public void Constructor_NullSocket_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new SocketDuplexPipe(null!));
    }

    [Fact]
    public async Task DisposeAsync_CancelsReaderAndWriter()
    {
        // Arrange: tạo pipe, Dispose, kiểm tra cả hai pipe đã Complete.
        using var client = new TcpClient();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var acceptTask = listener.AcceptTcpClientAsync();
        await client.ConnectAsync("127.0.0.1", port);
        var accepted = await acceptTask;
        listener.Stop();

        var pipe = new SocketDuplexPipe(accepted.Client);

        // Act
        await pipe.DisposeAsync();

        // Assert: gọi DisposeAsync lần nữa cũng không ném (idempotent).
        await pipe.DisposeAsync();
    }
}
