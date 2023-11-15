=== RUN   TestHandlerPauseAndResume
    amqpx_test.go:430:
                Error Trace:    /home/behm015/Development/amqpx/amqpx_test.go:430
                                                        /home/behm015/Development/amqpx/pool/subscriber.go:287
                                                        /home/behm015/Development/amqpx/pool/subscriber.go:222
                                                        /usr/local/go/src/runtime/asm_amd64.s:1650
                Error:          Not equal:
                                expected: false
                                actual  : true
                Test:           TestHandlerPauseAndResume
                Messages:       expected active to be false



panic: test timed out after 10m0s
running tests:
        TestBatchHandlerPauseAndResume (8m49s)

goroutine 316 [running]:
testing.(*M).startAlarm.func1()
        /usr/local/go/src/testing/testing.go:2259 +0x1fc
created by time.goFunc
        /usr/local/go/src/time/sleep.go:176 +0x45

goroutine 1 [chan receive, 8 minutes]:
testing.(*T).Run(0xc000082ea0, {0x89c94f, 0x1e}, 0x8c4010)
        /usr/local/go/src/testing/testing.go:1649 +0x856
testing.runTests.func1(0x0?)
        /usr/local/go/src/testing/testing.go:2054 +0x85
testing.tRunner(0xc000082ea0, 0xc0000f9908)
        /usr/local/go/src/testing/testing.go:1595 +0x239
testing.runTests(0xc00009abe0?, {0xb33520, 0x9, 0x9}, {0x4a8459?, 0x4a9c31?, 0xb397c0?})
        /usr/local/go/src/testing/testing.go:2052 +0x897
testing.(*M).Run(0xc00009abe0)
        /usr/local/go/src/testing/testing.go:1925 +0xb58
go.uber.org/goleak.VerifyTestMain({0x929a20, 0xc00009abe0}, {0xc0000f9e18, 0x3, 0x3})
        /home/behm015/go/pkg/mod/go.uber.org/goleak@v1.3.0/testmain.go:53 +0x65
github.com/jxsl13/amqpx_test.TestMain(0xfdfb0802185865db?)
        /home/behm015/Development/amqpx/amqpx_test.go:24 +0x2e9
main.main()
        _testmain.go:67 +0x308

goroutine 170 [semacquire, 7 minutes]:
sync.runtime_Semacquire(0xc0001287e8?)
        /usr/local/go/src/runtime/sema.go:62 +0x25
sync.(*WaitGroup).Wait(0xc0001287e0)
        /usr/local/go/src/sync/waitgroup.go:116 +0xa5
github.com/jxsl13/amqpx/pool.(*Subscriber).Close(0xc000128780)
        /home/behm015/Development/amqpx/pool/subscriber.go:35 +0x12a
github.com/jxsl13/amqpx.(*AMQPX).Close.(*AMQPX).close.func1()
        /home/behm015/Development/amqpx/amqpx.go:236 +0x96
sync.(*Once).doSlow(0xb398d4, 0xc0000ad888)
        /usr/local/go/src/sync/once.go:74 +0xf1
sync.(*Once).Do(0xb398d4, 0xc0000ad878?)
        /usr/local/go/src/sync/once.go:65 +0x45
github.com/jxsl13/amqpx.(*AMQPX).close(...)
        /home/behm015/Development/amqpx/amqpx.go:233
github.com/jxsl13/amqpx.(*AMQPX).Close(0xb39840)
        /home/behm015/Development/amqpx/amqpx.go:229 +0xf2
github.com/jxsl13/amqpx.Close(...)
        /home/behm015/Development/amqpx/amqpx.go:351
github.com/jxsl13/amqpx_test.testBatchHandlerPauseAndResume(0xc00029a4e0)
        /home/behm015/Development/amqpx/amqpx_test.go:751 +0x1331
github.com/jxsl13/amqpx_test.TestBatchHandlerPauseAndResume(0x0?)
        /home/behm015/Development/amqpx/amqpx_test.go:539 +0x31
testing.tRunner(0xc00029a4e0, 0x8c4010)
        /usr/local/go/src/testing/testing.go:1595 +0x239
created by testing.(*T).Run in goroutine 1
        /usr/local/go/src/testing/testing.go:1648 +0x82b

goroutine 34 [syscall, 9 minutes]:
os/signal.signal_recv()
        /usr/local/go/src/runtime/sigqueue.go:152 +0x29
os/signal.loop()
        /usr/local/go/src/os/signal/signal_unix.go:23 +0x1d
created by os/signal.Notify.func1.1 in goroutine 19
        /usr/local/go/src/os/signal/signal.go:151 +0x47

goroutine 33 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc00017e6e0, 0x1bf08eb00, 0xc0002d82a0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 291 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04e68, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc00014a4a0, 0xc000495000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc00014a480, {0xc000495000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc00014a480, {0xc000495000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc0002da170, {0xc000495000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc0002d95c0, {0xc00023a159, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc0002d95c0}, {0xc00023a159, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc000165f18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc0000566e0, {0x929fa0?, 0xc0002da170})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 41 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04d70, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc0001283a0, 0xc00029f000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000128380, {0xc00029f000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000128380, {0xc00029f000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc000158090, {0xc00029f000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc00014c780, {0xc00013c259, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc00014c780}, {0xc00013c259, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0000a9f18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc00017e2c0, {0x929fa0?, 0xc000158090})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 21 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc00017e2c0, 0x1bf08eb00, 0xc00008e660)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 58 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04c78, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc00034c120, 0xc000365000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc00034c100, {0xc000365000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc00034c100, {0xc000365000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc000346030, {0xc000365000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc000344240, {0xc0003cc6d9, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc000344240}, {0xc0003cc6d9, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc00016bf18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc00017e160, {0x929fa0?, 0xc000346030})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 137 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc000056000, 0x1bf08eb00, 0xc00008e420)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 238 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc000056840, 0x1bf08eb00, 0xc00008f3e0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 198 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc000056160, 0x1bf08eb00, 0xc000183b60)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 162 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04a88, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc000002120, 0xc0000d8000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000002100, {0xc0000d8000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000002100, {0xc0000d8000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc000346048, {0xc0000d8000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc00008e240, {0xc0004db449, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc00008e240}, {0xc0004db449, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0003ddf18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc000056000, {0x929fa0?, 0xc000346048})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 294 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04898, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc00014a620, 0xc0001c5000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc00014a600, {0xc0001c5000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc00014a600, {0xc0001c5000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc0002da1d0, {0xc0001c5000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc0002d98c0, {0xc0003cc459, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc0002d98c0}, {0xc0003cc459, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc000169f18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc000056840, {0x929fa0?, 0xc0002da1d0})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 77 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc00017e160, 0x1bf08eb00, 0xc000183200)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 123 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04b80, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc000128520, 0xc00033d000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000128500, {0xc00033d000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000128500, {0xc00033d000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc00017c110, {0xc00033d000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc000182e40, {0xc00052b5d9, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc000182e40}, {0xc00052b5d9, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0003d9f18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc00017e6e0, {0x929fa0?, 0xc00017c110})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 212 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce04990, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc000128620, 0xc000241000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000128600, {0xc000241000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000128600, {0xc000241000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc0003461b8, {0xc000241000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc00014cd80, {0xc000431469, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc00014cd80}, {0xc000431469, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0003e3f18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc000056160, {0x929fa0?, 0xc0003461b8})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 19
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 308 [sync.Mutex.Lock]:
sync.runtime_SemacquireMutex(0x929960?, 0x0?, 0xc0004252c0?)
        /usr/local/go/src/runtime/sema.go:77 +0x25
sync.(*Mutex).lockSlow(0xc0003c81d8)
        /usr/local/go/src/sync/mutex.go:171 +0x213
sync.(*Mutex).Lock(0xc0003c81d8)
        /usr/local/go/src/sync/mutex.go:90 +0x55
github.com/jxsl13/amqpx/pool.(*Connection).Recover(0xc0003c8160)
        /home/behm015/Development/amqpx/pool/connection.go:300 +0x48
github.com/jxsl13/amqpx/pool.(*Session).recover(0xc00014a100)
        /home/behm015/Development/amqpx/pool/session.go:211 +0x46
github.com/jxsl13/amqpx/pool.(*Session).Recover(0xc00014a100)
        /home/behm015/Development/amqpx/pool/session.go:193 +0x89
github.com/jxsl13/amqpx/pool.(*SessionPool).ReturnSession(0xc00014d260, 0xc00014a100, 0x1)
        /home/behm015/Development/amqpx/pool/session_pool.go:155 +0x77
github.com/jxsl13/amqpx/pool.(*Pool).ReturnSession(...)
        /home/behm015/Development/amqpx/pool/pool.go:107
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume.func1()
        /home/behm015/Development/amqpx/pool/subscriber.go:393 +0x22c
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume(0xc000128780, 0xc00014c240)
        /home/behm015/Development/amqpx/pool/subscriber.go:409 +0x6dc
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsumer(0xc000128780, 0xc00014c240, 0xc0001287e0)
        /home/behm015/Development/amqpx/pool/subscriber.go:359 +0x3e5
created by github.com/jxsl13/amqpx/pool.(*Subscriber).Start in goroutine 170
        /home/behm015/Development/amqpx/pool/subscriber.go:200 +0x5cf

goroutine 223 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc0000562c0, 0x1bf08eb00, 0xc0002d8480)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 278 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc0000566e0, 0x1bf08eb00, 0xc0001820c0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 306 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce046a8, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc0001285a0, 0xc000262000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000128580, {0xc000262000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000128580, {0xc000262000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc0002da0d0, {0xc000262000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc00014cf00, {0xc0003cc819, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc00014cf00}, {0xc0003cc819, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0003dff18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc000056580, {0x929fa0?, 0xc0002da0d0})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 255 [IO wait]:
internal/poll.runtime_pollWait(0x7f8b9ce047a0, 0x72)
        /usr/local/go/src/runtime/netpoll.go:343 +0x85
internal/poll.(*pollDesc).wait(0xc000128120, 0xc0003ea000?, 0x0)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:84 +0xb1
internal/poll.(*pollDesc).waitRead(...)
        /usr/local/go/src/internal/poll/fd_poll_runtime.go:89
internal/poll.(*FD).Read(0xc000128100, {0xc0003ea000, 0x1000, 0x1000})
        /usr/local/go/src/internal/poll/fd_unix.go:164 +0x3e5
net.(*netFD).Read(0xc000128100, {0xc0003ea000, 0x1000, 0x1000})
        /usr/local/go/src/net/fd_posix.go:55 +0x4b
net.(*conn).Read(0xc0002da030, {0xc0003ea000, 0x1000, 0x1000})
        /usr/local/go/src/net/net.go:179 +0xad
bufio.(*Reader).Read(0xc0002d8420, {0xc00046d2a9, 0x7, 0x7})
        /usr/local/go/src/bufio/bufio.go:244 +0x4be
io.ReadAtLeast({0x929c40, 0xc0002d8420}, {0xc00046d2a9, 0x7, 0x7}, 0x7)
        /usr/local/go/src/io/io.go:335 +0xd0
io.ReadFull(...)
        /usr/local/go/src/io/io.go:354
github.com/rabbitmq/amqp091-go.(*reader).ReadFrame(0xc0000abf18)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/read.go:49 +0x98
github.com/rabbitmq/amqp091-go.(*Connection).reader(0xc0000562c0, {0x929fa0?, 0xc0002da030})
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:726 +0x2ab
created by github.com/rabbitmq/amqp091-go.Open in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:271 +0x67a

goroutine 307 [select]:
github.com/rabbitmq/amqp091-go.(*Connection).heartbeater(0xc000056580, 0x1bf08eb00, 0xc000345da0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:761 +0x26d
created by github.com/rabbitmq/amqp091-go.(*Connection).openTune in goroutine 170
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:1016 +0xb8c

goroutine 310 [sync.Mutex.Lock]:
sync.runtime_SemacquireMutex(0x4b3cce?, 0xd8?, 0x4a9c69?)
        /usr/local/go/src/runtime/sema.go:77 +0x25
sync.(*Mutex).lockSlow(0xc0003c81d8)
        /usr/local/go/src/sync/mutex.go:171 +0x213
sync.(*Mutex).Lock(0xc0003c81d8)
        /usr/local/go/src/sync/mutex.go:90 +0x55
github.com/jxsl13/amqpx/pool.(*Connection).channel(0xc0003c8160)
        /home/behm015/Development/amqpx/pool/connection.go:356 +0x59
github.com/jxsl13/amqpx/pool.(*Session).connect(0xc000128700)
        /home/behm015/Development/amqpx/pool/session.go:164 +0x156
github.com/jxsl13/amqpx/pool.(*Session).recover(0xc000128700)
        /home/behm015/Development/amqpx/pool/session.go:219 +0x55
github.com/jxsl13/amqpx/pool.(*Session).Recover(0xc000128700)
        /home/behm015/Development/amqpx/pool/session.go:193 +0x89
github.com/jxsl13/amqpx/pool.(*SessionPool).ReturnSession(0xc00014d260, 0xc000128700, 0x1)
        /home/behm015/Development/amqpx/pool/session_pool.go:155 +0x77
github.com/jxsl13/amqpx/pool.(*Pool).ReturnSession(...)
        /home/behm015/Development/amqpx/pool/pool.go:107
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume.func1()
        /home/behm015/Development/amqpx/pool/subscriber.go:393 +0x22c
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume(0xc000128780, 0xc00014c300)
        /home/behm015/Development/amqpx/pool/subscriber.go:409 +0x6dc
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsumer(0xc000128780, 0xc00014c300, 0xc0001287e0)
        /home/behm015/Development/amqpx/pool/subscriber.go:359 +0x3e5
created by github.com/jxsl13/amqpx/pool.(*Subscriber).Start in goroutine 170
        /home/behm015/Development/amqpx/pool/subscriber.go:200 +0x5cf

goroutine 309 [runnable]:
github.com/rabbitmq/amqp091-go.(*allocator).reserve(0xc000505fa0, 0x6d0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/allocator.go:103 +0x105
github.com/rabbitmq/amqp091-go.(*allocator).next(0xc000505fa0)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/allocator.go:84 +0x176
github.com/rabbitmq/amqp091-go.(*Connection).allocateChannel(0xc000056580)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:819 +0xf6
github.com/rabbitmq/amqp091-go.(*Connection).openChannel(0xc0003c81d8?)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:847 +0x33
github.com/rabbitmq/amqp091-go.(*Connection).Channel(...)
        /home/behm015/go/pkg/mod/github.com/rabbitmq/amqp091-go@v1.9.0/connection.go:873
github.com/jxsl13/amqpx/pool.(*Connection).channel(0xc0003c8160)
        /home/behm015/Development/amqpx/pool/connection.go:358 +0xb6
github.com/jxsl13/amqpx/pool.(*Session).connect(0xc00014a180)
        /home/behm015/Development/amqpx/pool/session.go:164 +0x156
github.com/jxsl13/amqpx/pool.(*Session).recover(0xc00014a180)
        /home/behm015/Development/amqpx/pool/session.go:219 +0x55
github.com/jxsl13/amqpx/pool.(*Session).Recover(0xc00014a180)
        /home/behm015/Development/amqpx/pool/session.go:193 +0x89
github.com/jxsl13/amqpx/pool.(*SessionPool).ReturnSession(0xc00014d260, 0xc00014a180, 0x1)
        /home/behm015/Development/amqpx/pool/session_pool.go:155 +0x77
github.com/jxsl13/amqpx/pool.(*Pool).ReturnSession(...)
        /home/behm015/Development/amqpx/pool/pool.go:107
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume.func1()
        /home/behm015/Development/amqpx/pool/subscriber.go:393 +0x22c
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsume(0xc000128780, 0xc00014c2a0)
        /home/behm015/Development/amqpx/pool/subscriber.go:409 +0x6dc
github.com/jxsl13/amqpx/pool.(*Subscriber).batchConsumer(0xc000128780, 0xc00014c2a0, 0xc0001287e0)
        /home/behm015/Development/amqpx/pool/subscriber.go:359 +0x3e5
created by github.com/jxsl13/amqpx/pool.(*Subscriber).Start in goroutine 170
        /home/behm015/Development/amqpx/pool/subscriber.go:200 +0x5cf
FAIL    github.com/jxsl13/amqpx 600.024s
FAIL