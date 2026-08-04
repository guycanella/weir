# WR-021 — Worker Skeleton: Plan of Action

## Task definition

From `IMPLEMENTATION.md`:

- **Do:** implement `cmd/worker` with SQS long polling, context-based
  cancellation, and SIGTERM graceful shutdown that finishes in-flight work.
- **Done when:** the worker drains a test queue and shuts down cleanly on
  signal.

WR-021 implements a serial consume/delete skeleton. It establishes the worker
process, lifecycle, and test seams without pulling later processing,
concurrency, retry, telemetry, or image-building work into this task.

## Files

```text
cmd/worker/main.go
internal/worker/worker.go
internal/worker/worker_test.go
internal/worker/worker_integration_test.go
```

- `cmd/worker/main.go` contains process wiring only.
- `internal/worker/worker.go` contains the reusable, testable consume loop.
- `internal/worker/worker_test.go` contains fast in-process unit tests.
- `internal/worker/worker_integration_test.go` contains the LocalStack and
  subprocess test behind `//go:build integration`.

## Worker API

```go
type ProcessFunc func(context.Context, awsclient.Message) error

type Worker struct {
    SQSClient     awsclient.SQSClient
    QueueURL      string
    Process       ProcessFunc
    ShutdownGrace time.Duration
}

func (w *Worker) Run(recvCtx context.Context) error
```

The receive settings are constants for WR-021:

- `MaxNumberOfMessages`: 10
- `WaitTimeSeconds`: 20

`VisibilityTimeout` is not overridden by the worker. SQS uses the queue's
configured value; visibility tuning belongs to WR-024.

The worker validates its required dependencies and configuration before
starting. `ShutdownGrace` defaults to 30 seconds through construction or
process wiring; tests may supply a shorter duration.

## Context and shutdown model

The worker uses two distinct lifetimes.

### Receive context

`recvCtx` comes from `signal.NotifyContext` in `main`. It is used only by
`ReceiveMessage`.

Cancellation therefore:

- interrupts an active long poll immediately;
- prevents the worker from receiving another batch; and
- is treated as normal shutdown rather than a worker failure.

An arbitrary receive error is not treated as cancellation merely because it
is `context.DeadlineExceeded`. It is a clean shutdown only when `recvCtx.Err()`
is non-nil. Other receive errors are returned. WR-021 does not add an
unbounded retry loop without a defined backoff policy.

### Work context

`workCtx` is initially active without a deadline and is passed to
`ProcessFunc` and `DeleteMessage`. It is not canceled immediately by SIGTERM,
so the batch already returned by SQS can finish.

When `recvCtx.Done()` closes, a watchdog starts one shared
`ShutdownGrace` timer. If the current batch has not completed before the timer
expires, the watchdog cancels `workCtx` with a sentinel cause:

```go
var ErrShutdownTimeout = errors.New("worker shutdown grace expired")

workCtx, cancelWork := context.WithCancelCause(context.Background())
```

The implementation checks `context.Cause(workCtx)` for
`ErrShutdownTimeout`. It must not expect `context.DeadlineExceeded`:
`WithCancelCause` reports cancellation while preserving the explicit cause.

The watchdog has an acknowledgement channel owned by `Run`. On every return
path, `Run` closes that channel, stops any active timer, and prevents a
goroutine from surviving the worker invocation.

Conceptually:

```go
watchDone := make(chan struct{})
defer close(watchDone)

go func() {
    select {
    case <-recvCtx.Done():
        timer := time.NewTimer(w.ShutdownGrace)
        defer timer.Stop()

        select {
        case <-timer.C:
            cancelWork(ErrShutdownTimeout)
        case <-watchDone:
        }
    case <-watchDone:
    }
}()
```

The real implementation must arrange ownership so `watchDone` is closed
exactly once and `cancelWork(nil)` is deferred safely.

## Run state machine

For each iteration:

1. If `recvCtx` is already canceled and no batch is pending, return `nil`.
2. Call `ReceiveMessage` with `recvCtx`, a batch size of 10, and a 20-second
   long poll.
3. If receive returns after `recvCtx` cancellation, return `nil`.
4. If receive returns another error, wrap and return it.
5. Process the returned batch serially using `workCtx`.
6. After `ProcessFunc` succeeds, delete that message using `workCtx`.
7. A processing or deletion failure leaves the message undeleted for eventual
   redelivery and does not prevent the rest of the batch from being attempted.
8. If shutdown was requested while processing, finish the received batch
   within the shared grace period, then return `nil` without another receive.
9. If the grace period expires, stop iterating immediately and return an error
   wrapping `ErrShutdownTimeout`.

The timeout error reports the number of messages at or after the current batch
position:

```go
len(out.Messages) - currentIndex
```

It does not query approximate queue attributes and does not infer timeout from
undeleted messages. A message may be undeleted because of an ordinary
processing or deletion failure, which is different from shutdown timing out.

The serial loop does not need a separate pending queue or drain function. The
slice returned by `ReceiveMessage` is the in-flight batch. After SIGTERM, the
worker continues through that slice with `workCtx` and then exits.

## `cmd/worker/main.go`

The binary:

1. Reads and trims:
   - `QUEUE_URL`
   - `AWS_REGION`
   - `AWS_ENDPOINT_URL`
2. Fails startup with a non-zero exit if `QUEUE_URL` or `AWS_REGION` is empty.
3. Builds `awssdk.Config` and calls `awssdk.NewClients`.
4. Creates a context with:

   ```go
   signal.NotifyContext(
       context.Background(),
       syscall.SIGINT,
       syscall.SIGTERM,
   )
   ```

5. Constructs the worker with a 30-second shutdown grace.
6. Supplies a temporary processing stub that logs only `message_id` through
   `slog` and returns `nil`.
7. Calls `Run` and exits non-zero for an actual worker or shutdown-timeout
   error.

WR-021 does not add a `SHUTDOWN_GRACE` environment variable. Unit tests set a
short grace directly on `Worker`; runtime configuration can be introduced
later when required.

Message bodies are not logged because they may contain user data. Structured
logging configuration and assertions remain part of WR-025.

## Unit tests

Unit tests use `internal/awsclient/fake` where its behavior matches the
scenario and small test-only wrappers where lifecycle control is required.
All tests terminate deterministically; none rely on sleeps to win races.

### `TestRunConsumesAllMessages`

- Seed three messages in the fake queue.
- Cancel the receive context from the third processing callback.
- Assert all three receipt handles were deleted.
- Assert `Run` returns `nil`.

Cancellation is necessary because the fake returns an empty batch immediately
instead of performing a real long poll.

### `TestCancelBeforeReceive`

- Pass an already-canceled receive context.
- Assert receive is not invoked.
- Assert `Run` returns `nil`.

### `TestCancelDuringReceive`

Use a test-only wrapper that announces when receive starts, blocks until the
context is canceled, and returns `ctx.Err()`:

```go
type blockingSQS struct {
    awsclient.SQSClient
    started chan struct{}
}

func (b *blockingSQS) ReceiveMessage(
    ctx context.Context,
    in awsclient.ReceiveMessageInput,
) (awsclient.ReceiveMessageOutput, error) {
    close(b.started)
    <-ctx.Done()
    return awsclient.ReceiveMessageOutput{}, ctx.Err()
}
```

The test waits for `started` before canceling. No seeded message is required.
This verifies propagation through a receive actually in flight; injecting a
synthetic context error would only test error classification.

### `TestCancelAfterReceiveDrainsCurrentBatch`

- Seed five messages.
- Cancel `recvCtx` from the first processing callback.
- Allow every processor and deletion to complete within the grace period.
- Assert all five messages are deleted.
- Assert no second receive occurs and `Run` returns `nil`.

This is the primary deterministic proof that cancellation stops new receives
without abandoning the current batch.

### `TestShutdownGraceExpiresMidBatch`

- Let the first message complete.
- Make the second processor block on `workCtx.Done()`.
- Cancel `recvCtx`.
- Configure a short shutdown grace.
- Assert the processor observes cancellation caused by
  `ErrShutdownTimeout`.
- Assert the third processor is never invoked.
- Assert `Run` returns an error wrapping `ErrShutdownTimeout` with the expected
  remaining count.

### `TestReceiveError`

- Inject a non-context receive error with an active `recvCtx`.
- Assert it is wrapped and returned.
- Assert no processing or deletion occurs.

### `TestProcessErrorSkipsDelete`

- Return a sentinel processing error for the second of three messages.
- Cancel deterministically after the third callback.
- Assert the first and third messages are deleted.
- Assert the second remains undeleted.

### `TestDeleteErrorLeavesMessageUndeleted`

- Inject a one-shot `DeleteMessage` error.
- Assert the worker continues through the rest of the batch.
- Assert the failed deletion remains absent from the fake's deletion record.

## Integration test

`internal/worker/worker_integration_test.go` is gated by:

```go
//go:build integration
```

The test:

1. Uses the existing LocalStack environment and AWS client conventions.
2. Creates a unique test queue with a short visibility timeout, such as five
   seconds.
3. Registers cleanup immediately so the queue is removed even after failure.
4. Sends ten messages through `awssdk.Clients.SQS.SendMessage`.
5. Builds `./cmd/worker` into `t.TempDir()` and starts that binary as a
   subprocess. It does not use `go run`, whose wrapper complicates signal
   handling.
6. Supplies `QUEUE_URL`, `AWS_REGION`, `AWS_ENDPOINT_URL`, and the existing
   LocalStack test credentials explicitly.
7. Polls queue attributes until both values reach zero:
   - `ApproximateNumberOfMessages`
   - `ApproximateNumberOfMessagesNotVisible`
8. Uses a bounded test deadline and a short polling interval rather than a
   fixed startup sleep.
9. Sends SIGTERM after the drain is observed.
10. Asserts the subprocess exits within the deadline with exit code 0.

Checking visible and not-visible counts prevents a received-but-undeleted
message from masquerading as a successfully drained queue. The short queue
visibility timeout also keeps any failure diagnosis bounded.

The integration test proves:

- real environment and AWS client wiring;
- LocalStack long-poll consumption and deletion;
- binary-level SIGTERM handling; and
- clean process exit.

The in-process tests, rather than this subprocess test, provide the
deterministic proof of cancellation during a batch and grace-period expiry.

## Error behavior

- Expected cancellation while receiving: return `nil`.
- Receive failure unrelated to shutdown: wrap and return.
- Processing failure: leave the message undeleted and continue the batch.
- Deletion failure: leave the message undeleted and continue the batch.
- Shutdown grace expiry: stop the batch immediately and return an error
  wrapping `ErrShutdownTimeout`.

Comprehensive retry, backoff, poison-message, visibility-extension, and DLQ
behavior is deferred to WR-024.

## Deliberate scope cuts

- Bounded concurrency and goroutines/semaphore: WR-022.
- Pluggable real processing, S3 output, and idempotency: WR-023 and WR-010.
- Retry, DLQ, poison-message, and visibility handling: WR-024.
- Structured logging configuration, OpenTelemetry spans, and metrics: WR-025.
- Worker image and `ko` integration: WR-026.
- Full publish-to-result end-to-end worker test: WR-027.

## Acceptance checklist

- `go build ./cmd/worker` succeeds.
- Unit tests prove serial consumption and delete-after-success.
- Cancellation of a long poll returns cleanly.
- SIGTERM during an in-flight batch stops new receives and finishes that batch.
- The batch drain uses one bounded shutdown-grace budget.
- Grace expiry cancels cooperative processing, stops remaining batch work, and
  returns a distinct timeout error.
- The integration test drains both visible and in-flight LocalStack messages.
- The compiled subprocess handles SIGTERM and exits with status 0.
- No WR-022 through WR-027 behavior is implemented prematurely.
