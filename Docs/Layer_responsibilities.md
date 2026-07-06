# PipeX — Công việc của từng lớp kiến trúc

Tài liệu này tóm tắt lại **công việc cụ thể** (không phải chỉ API) mà mỗi lớp trong
kiến trúc 4 tầng của PipeX phải đảm nhiệm, để dùng làm cơ sở viết unit test và
review code. Xem sơ đồ luồng dữ liệu ở phần đầu cuộc trò chuyện để hình dung
trực quan quan hệ giữa các lớp.

## Nguyên tắc bất biến (áp dụng cho mọi lớp)

1. **Không nhìn xuyên quá 1 tầng.** Codec không bao giờ đụng `Socket`; Protocol
   không đụng `PipeReader`/`PipeWriter` thô.
2. **Composition, không inheritance.** Không có class nào phải kế thừa một base
   class của framework để "cắm" vào hệ thống — mọi thứ là interface nhỏ
   (`IDuplexPipe`, `IFrameDecoder<T>`, `IFrameEncoder<T>`) được truyền vào qua
   tham số hoặc extension method.
3. **Không có buffer bị "mồ côi".** Mọi `ReadOnlySequence<byte>`/`Memory<byte>`
   phải được `Advance`/`Complete` đúng cách, kể cả khi có exception — nếu không
   sẽ rò rỉ bộ nhớ pool hoặc treo `PipeReader`.
4. **Backpressure là mặc định, không phải tính năng thêm.** Bất kỳ đường ghi nào
   (Codec → Transport → Socket) đều phải tôn trọng `pauseWriterThreshold` mà
   không cần tầng trên biết gì về nó.

---

## Lớp 1 — .NET BCL / CLR / OS

**Công việc:** không phải code của PipeX — đây là nền tảng execution
(`ThreadPool`, IOCP trên Windows / epoll trên Linux, `Socket`). PipeX không viết
gì ở tầng này, chỉ **tiêu thụ đúng API bất đồng bộ** của nó
(`Socket.ReceiveAsync`, `Socket.SendAsync`, `Socket.AcceptAsync`).

**Việc cần kiểm chứng gián tiếp (qua Lớp 2):** mỗi kết nối phải chạy trên 1
`Task` được ThreadPool lên lịch động (work-stealing), không ghim cứng vào một
thread cố định như mô hình `EventLoopGroup` cũ.

---

## Lớp 2 — PipeX.Transport

### T1. `IDuplexPipe` / `SocketDuplexPipe`

| | |
|---|---|
| **Mục tiêu** | Che giấu hoàn toàn `Socket` sau một cặp `PipeReader`/`PipeWriter`. |
| **Input** | `Socket` đã `Accept`/`Connect`. |
| **Output** | `Input` (đọc byte tới) và `Output` (ghi byte đi), đều bất đồng bộ. |
| **Việc phải làm đúng** | (1) Bơm dữ liệu từ `Socket.ReceiveAsync` vào `PipeWriter` nội bộ và `FlushAsync` sau mỗi lần `Advance`; (2) khi socket đóng hoặc nhận 0 byte, phải `Complete()` cả hai chiều để `ReadFramesAsync` phía trên thoát vòng lặp thay vì treo mãi; (3) khi `DisposeAsync`, phải hủy pump task và giải phóng `Socket`, không rò task chạy nền. |
| **Việc cần test** | Round-trip byte qua một cặp pipe giả lập (không cần socket thật); pipe phải hoàn tất (`IsCompleted`) khi phía ghi gọi `Complete()`. |

### T2. `PipeXListener`

| | |
|---|---|
| **Mục tiêu** | Accept-loop: mỗi kết nối mới = 1 `Task` độc lập, không chặn vòng lặp accept. |
| **Input** | `EndPoint` để bind, delegate `onConnected(IDuplexPipe, CancellationToken)`. |
| **Output** | Với mỗi kết nối: gọi `onConnected` trên một `Task` riêng; khi `onConnected` kết thúc (bình thường hoặc lỗi) phải `Shutdown` socket. |
| **Việc phải làm đúng** | (1) Không được `await HandleAsync(...)` trong vòng lặp accept — nếu không, kết nối thứ 2 sẽ đợi kết nối thứ 1 xử lý xong; (2) exception ném ra từ `onConnected` không được làm chết accept-loop; (3) khi `CancellationToken` bị hủy, `AcceptAsync` phải dừng lại gọn gàng. |
| **Việc cần test** | Hai kết nối "khách" gửi đến gần như đồng thời phải được xử lý song song (kết nối 2 không đợi kết nối 1 hoàn tất); listener không sập khi handler ném exception. |

### T3. `PipeOptions` / Backpressure

| | |
|---|---|
| **Mục tiêu** | Cấp phát bộ nhớ không rác (pool) + chặn writer khi reader đọc chậm. |
| **Input** | `pauseWriterThreshold` (mặc định 1 MiB), `resumeWriterThreshold` (mặc định 512 KiB), `MemoryPool<byte>.Shared`. |
| **Output** | `FlushAsync()` trả `Task` chưa hoàn tất khi vượt ngưỡng pause; hoàn tất lại khi reader đọc xuống dưới ngưỡng resume. |
| **Việc phải làm đúng** | `resumeWriterThreshold` phải **nhỏ hơn** `pauseWriterThreshold` (nếu không backpressure sẽ dao động liên tục hoặc không bao giờ nhả). |
| **Việc cần test** | Ghi vượt `pauseWriterThreshold` vào một `Pipe` cấu hình từ `PipeXPipeOptions` → `FlushAsync` phải chưa hoàn tất; đọc bớt xuống dưới `resumeWriterThreshold` → `FlushAsync` phải hoàn tất. |

---

## Lớp 3 — PipeX.Codec

### C1. `IFrameDecoder<T>` + `ReadFramesAsync`

| | |
|---|---|
| **Mục tiêu** | Chuẩn hoá vòng lặp "đọc thêm → thử decode → lặp tới hết frame", phơi ra dạng `IAsyncEnumerable<T>` (pull-based). |
| **Input** | `ReadOnlySequence<byte>` tích luỹ từ `PipeReader.ReadAsync()`, có thể đa đoạn (multi-segment). |
| **Output** | Chuỗi `T` hoàn chỉnh; phần dữ liệu dư (frame cắt đôi giữa 2 lần đọc) phải được giữ nguyên, không copy. |
| **Việc phải làm đúng** | (1) `AdvanceTo(buffer.Start, buffer.End)` phải dùng đúng `examined` để tránh vòng lặp bận (busy loop) khi chưa đủ dữ liệu; (2) phải decode **hết** các frame có sẵn trong buffer trước khi đọc thêm (vòng `while (decoder.TryDecode(...))`); (3) khi `result.IsCompleted`, phải dừng đúng lúc, không được bỏ sót frame cuối cùng nếu nó đã đủ dữ liệu. |
| **Việc cần test** | Buffer chứa đúng 1 frame → trả về đúng 1 phần tử; buffer chứa 2 frame trong 1 lần đọc → trả về đúng 2 phần tử theo thứ tự; frame bị cắt đôi giữa 2 lần ghi → chỉ trả về sau khi phần còn lại tới. |

### C2. Decoder cụ thể — Length-field / Delimiter / Chunked

| Decoder | Việc phải làm đúng | Việc cần test |
|---|---|---|
| `LengthFieldFrameDecoder` | Từ chối (throw) khi frame vượt `maxFrameLength`, tránh OOM do peer gửi length field giả mạo. | Frame hợp lệ → decode đúng; length field vượt `maxFrameLength` → ném exception thay vì cố cấp phát. |
| `DelimiterFrameDecoder` | Không được bỏ sót delimiter nằm ở ranh giới giữa 2 segment của `ReadOnlySequence`. | Delimiter nằm trọn trong 1 segment; delimiter bị chia đôi giữa 2 segment (dùng `ReadOnlySequenceSegment` giả lập) — cả hai phải cho cùng kết quả. |
| `ChunkedFrameDecoder` | Phải ghép đúng nhiều chunk (theo RFC 6242) thành 1 message hoàn chỉnh, không trộn lẫn dữ liệu giữa các message liên tiếp. | 1 message gồm nhiều chunk gửi rời rạc → ghép đúng thứ tự và đúng nội dung. |

### C3. `IFrameEncoder<T>` + `WriteFrameAsync`

| | |
|---|---|
| **Mục tiêu** | Chiều ngược lại của C1: chuyển message tầng Protocol thành bytes, ghi qua `PipeWriter`. |
| **Input** | Message `T`, `IBufferWriter<byte>` (chính là `PipeWriter.Output`). |
| **Output** | Bytes đã `FlushAsync`; nếu `FlushResult.IsCanceled`, phải ném `OperationCanceledException` để tầng gọi biết ghi thất bại thay vì âm thầm bỏ qua. |
| **Việc cần test** | Encode 1 message → bytes ghi ra đúng định dạng byte-for-byte kỳ vọng; `FlushAsync` bị hủy (writer `CancelPendingFlush()`) → ném đúng exception. |

### C4. `IdleWatch`

| | |
|---|---|
| **Mục tiêu** | Phát hiện kết nối không có hoạt động đọc/ghi trong X giây, dùng `TimeProvider` để test được bằng fake-time thay vì phải chờ đồng hồ thật. |
| **Input** | `TimeProvider`, `idleTimeout`; `Touch()` gọi mỗi khi có frame đọc/ghi thành công. |
| **Output** | `IAsyncEnumerable<Unit>` — mỗi lần tick vượt `idleTimeout` kể từ lần `Touch()` gần nhất thì yield 1 sự kiện. |
| **Việc phải làm đúng** | (1) Gọi `Touch()` phải reset lại bộ đếm — nếu hoạt động liên tục, không bao giờ được phát sự kiện idle; (2) không phát sự kiện idle "giả" ngay khi khởi tạo nếu `idleTimeout` chưa trôi qua. |
| **Việc cần test** | Dùng `TimeProvider` giả (advance thời gian thủ công): không `Touch()` → sau khi advance qua `idleTimeout`, phải nhận được đúng 1 sự kiện; có `Touch()` liên tục trước mỗi lần advance → không nhận sự kiện nào. |

---

## Lớp 4 — Điểm neo cho Protocol (PipeX.Ssh / PipeX.Netconf)

### P1. Protocol read-loop

**Công việc:** chỉ làm 2 việc — (1) chọn decoder/encoder phù hợp, (2) chạy state
machine nghiệp vụ trên message đã decode. Không được đụng `byte[]`/`Socket` thô.

**Việc cần test:** với một cặp `IDuplexPipe` giả lập nối vòng (client ghi gì,
server đọc nấy), gửi 1 request → nhận đúng 1 response tương ứng; khi không có
hoạt động trong khoảng `idleTimeout`, phải kích hoạt nhánh keep-alive
(`Task.WhenAny` với `idles.MoveNextAsync()` thắng trước `frames.MoveNextAsync()`).

> **Lưu ý khi triển khai:** bản phác thảo ban đầu của vòng lặp P1 gọi
> `frames.MoveNextAsync()` mới ở **mỗi** vòng lặp, kể cả khi nhánh idle thắng
> race và `continue`. Đó là lỗi tinh vi: `IAsyncEnumerator.MoveNextAsync()`
> không được gọi lần nữa khi lần gọi trước đó chưa hoàn tất — vi phạm hợp đồng
> này thường ném `InvalidOperationException` lúc chạy. Bản triển khai trong
> `EchoSessionRunner.cs` sửa lỗi này bằng cách giữ lại `Task<bool>?
> frameMoveNextTask` đang chờ giữa các vòng lặp, chỉ tạo lời gọi
> `MoveNextAsync()` mới sau khi kết quả trước đó đã được tiêu thụ hoàn toàn.

### P2. `SshSecureDuplexPipe` — đổi cipher runtime sau KEX

| | |
|---|---|
| **Mục tiêu** | Cho phép đổi `ICipher` đang dùng ngay sau khi key exchange (KEX) hoàn tất, mà không cần "gỡ/lắp lại" handler nào — decorator giữ nguyên interface `IDuplexPipe`. |
| **Input** | `IDuplexPipe` gốc (chưa mã hoá) + `ICipher` hiện hành (đổi được tại chỗ qua property `ActiveCipher`). |
| **Output** | Vẫn là một `IDuplexPipe` — chỉ khác là đọc/ghi qua nó sẽ tự động giải mã/mã hoá bằng `ActiveCipher` đang có tại thời điểm gọi. |
| **Việc phải làm đúng** | Đổi `ActiveCipher` giữa chừng phiên làm việc không được làm hỏng dữ liệu **đã** mã hoá bằng cipher cũ đang nằm trong buffer chưa đọc — cipher mới chỉ áp dụng cho dữ liệu **mới** đọc/ghi sau thời điểm đổi. |
| **Việc cần test** | Ghi dữ liệu bằng cipher A, đổi sang cipher B, ghi tiếp dữ liệu khác → đọc lại từ phía kia phải giải mã đúng từng đoạn bằng đúng cipher đã dùng khi ghi (không lẫn cipher). |

---

## Bảng ánh xạ Module → File test tương ứng

| Module | File nguồn | File test |
|---|---|---|
| T1 | `src/PipeX.Transport/IDuplexPipe.cs`, `InMemoryDuplexPipe.cs` | `tests/PipeX.Transport.Tests/InMemoryDuplexPipeTests.cs` |
| T2 | `src/PipeX.Transport/PipeXListener.cs` | `tests/PipeX.Transport.Tests/PipeXListenerTests.cs` |
| T3 | `src/PipeX.Transport/PipeXPipeOptions.cs` | `tests/PipeX.Transport.Tests/BackpressureTests.cs` |
| C1 | `src/PipeX.Codec/IFrameDecoder.cs`, `FrameReaderExtensions.cs` | `tests/PipeX.Codec.Tests/FrameReaderExtensionsTests.cs` |
| C2 | `src/PipeX.Codec/LengthFieldFrameDecoder.cs`, `DelimiterFrameDecoder.cs`, `ChunkedFrameDecoder.cs` | `tests/PipeX.Codec.Tests/LengthFieldFrameDecoderTests.cs`, `DelimiterFrameDecoderTests.cs`, `ChunkedFrameDecoderTests.cs` |
| C3 | `src/PipeX.Codec/IFrameEncoder.cs`, `FrameWriterExtensions.cs` | `tests/PipeX.Codec.Tests/FrameWriterExtensionsTests.cs` |
| C4 | `src/PipeX.Codec/IdleWatch.cs` | `tests/PipeX.Codec.Tests/IdleWatchTests.cs` |
| P1 | `src/PipeX.Protocol/EchoSessionRunner.cs` | `tests/PipeX.Protocol.Tests/EchoSessionRunnerTests.cs` |
| P2 | `src/PipeX.Protocol/SshSecureDuplexPipe.cs`, `Ciphers.cs` | `tests/PipeX.Protocol.Tests/SshSecureDuplexPipeTests.cs` |

> **Ghi chú về môi trường chạy test:** bộ test dùng xUnit (`dotnet test` từ thư
> mục gốc solution). Sandbox hiện tại dùng để soạn tài liệu này **không có
> .NET SDK cài sẵn và không có quyền truy cập mạng** để tải NuGet packages, nên
> các file test dưới đây được viết cẩn thận theo đúng API/behaviour mô tả ở
> trên nhưng **chưa được build/run thử**. Khuyến nghị chạy `dotnet test` trên
> máy của bạn trước khi merge để bắt các lỗi biên dịch nhỏ (nếu có).