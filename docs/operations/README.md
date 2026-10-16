# Orbit Streams operations

The package transforms batches already supplied by the application; it does not connect to a
broker. Integrate a source adapter explicitly and define whether the adapter acknowledges messages,
commits offsets, or checkpoints only after downstream handling succeeds.

## Batch and concurrency capacity

Default maximums are 1,024 records and 1 MiB of accounted fields per batch. A value is capped at
256 KiB and a key at 4 KiB. Each record allows at most 16 headers and 4 KiB combined header bytes.
These counts do not include interpreter overhead, source client buffers, copies made by a
processor, or Protobuf framing. The process adapter separately enforces a 1 MiB serialized request
and response limit.

`process_many()` defaults to eight in-flight batches and accepts an explicit concurrency between
one and 128. This is a per-call bound, not a global application capacity limit. Multiple concurrent
calls can exceed it; establish an application-wide admission limit where required. Lower
concurrency reduces memory and downstream pressure, while increasing it can improve utilization
for I/O-bound processors. Output order follows source order, so a slow early batch can hold back
later results even when those processors have completed.

## Lifecycle and cancellation

The pipeline owns no network client or processor lifecycle. The application must close any
resources used by a processor. `ProcessStreamProcessor.aclose()` marks that adapter closed and
rejects later calls; it does not stop the Core process. Core owns local plugin startup, health, and
shutdown. Coordinate application shutdown so no new batches are submitted, in-flight work is
drained or cancelled within the service deadline, and owned resources are then closed.

Pipeline cancellation propagates to active processor calls. `process_many()` cancels and joins
outstanding tasks after cancellation or failure. Custom processors must not swallow cancellation
or perform unbounded synchronous work on the event loop. A process RPC has a configurable deadline
(default five seconds, range 50 ms to 60 seconds); timeout does not prove the remote process had no
side effects.

## Failure and replay

Processor failures are sanitized and identify the pipeline stage; process adapter failures and
invalid Protobuf output also fail the batch. No component retries automatically. Replaying a failed
batch can repeat side effects, and this package does not coordinate source offsets with downstream
writes. Keep processors pure where possible. Otherwise, use application-level idempotency or a
transaction protocol supported by both the selected source and sink.

## Production validation

Test representative data volumes, burst concurrency, slow processors, cancellation, process
startup and shutdown, RPC deadlines, malformed plugin output, and source acknowledgement ordering.
Observe queue lag, batch sizes, processing latency, failures, and process health through the host's
metrics/logging/tracing integrations. The package exports no broker, durable checkpoint service,
metrics endpoint, or health endpoint; do not infer their existence from the batch API.
