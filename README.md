# PipeX
An asynchronous, high-performance network framework for modern .NET—built on System.IO.Pipelines with forced back pressure and zero-allocation memory ownership.

## Features
- Asynchronous, high-performance networking
- Non-blocking I/O with System.IO.Pipelines
- Forced back pressure to prevent memory overuse
- Zero-allocation memory ownership for efficient resource management
- Cross-platform support for Windows, Linux, and macOS

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  LỚP 4 — Protocol  (PipeX.Ssh, PipeX.Netconf — giai đoạn sau)      │
│  Chỉ nhìn thấy: IDuplexPipe, IAsyncEnumerable<TFrame>,             │
│                 IFrameDecoder<T>/IFrameEncoder<T>                  │
└───────────────────────────┬────────────────────────────────────────┘
                            │ await foreach (var f in reader.ReadFramesAsync(codec, ct))
                            │ await writer.WriteFrameAsync(codec, frame, ct)
┌───────────────────────────┴────────────────────────────────────────┐
│  LỚP 3 — PipeX.Codec                                               │
│  Chỉ nhìn thấy: PipeReader, PipeWriter, ReadOnlySequence<byte>,    │
│                 IBufferWriter<byte>, TimeProvider                  │
└───────────────────────────┬────────────────────────────────────────┘
                            │ IDuplexPipe.Input / IDuplexPipe.Output
┌───────────────────────────┴────────────────────────────────────────┐
│  LỚP 2 — PipeX.Transport                                           │
│  Chỉ nhìn thấy: Socket, System.IO.Pipelines.Pipe, PipeOptions      │
│  (tuỳ chọn: Microsoft.AspNetCore.Connections.Abstractions)         │
└───────────────────────────┬────────────────────────────────────────┘
                            │ Socket.AcceptAsync/ReceiveAsync/SendAsync,
                            │ System.Threading.Channels (nếu cần hàng đợi), TimeProvider
┌───────────────────────────┴────────────────────────────────────────┐
│  LỚP 1 — .NET BCL / CLR / OS  (ThreadPool + IOCP/epoll)            │
└────────────────────────────────────────────────────────────────────┘
```
