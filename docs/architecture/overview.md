# Orbit Streams architecture

`orbit-streams` is a provider-neutral, in-process batch transformation capability. It defines
bounded immutable records, a processor protocol, an ordered pipeline, and an optional adapter for
invoking a Core-supervised process implementation over the typed Protobuf contract. It is not a
broker consumer, durable stream engine, or stream-processing cluster.

```text
source/broker adapter ──> StreamBatch ──> StreamPipeline ──> application sink
                                   └── optional ProcessStreamProcessor
                                        └── Core-supervised orbit.streams plugin
```

The application or broker adapter owns reads, offsets, acknowledgements, checkpoints, retries,
ordering between batches, and durable output. A source should be acknowledged or committed only
after the application has successfully handled the transformed output. Whether this is safe across
crashes depends on the source and sink transaction model; this package does not provide one.

## Data model and bounds

`StreamRecord` contains a bounded ASCII record identifier, bytes value, non-negative signed 64-bit
millisecond timestamp, optional bytes key, printable ASCII content type, and immutable bounded
headers. The value is at most 256 KiB; the key is at most 4 KiB; a record has at most 16 headers;
each header value is at most 256 UTF-8 bytes; and header name/value bytes total at most 4 KiB.

`StreamBatch` accepts at most 1,024 records and accounts at most 1 MiB of user-controlled fields
across the records. The accounting includes values, keys, IDs, content types, and headers, but not
Python object overhead or Protobuf wire framing. `ProcessStreamProcessor` separately rejects
requests and responses over the 1 MiB Protobuf wire limit. A configured lower batch bound is
preserved across every pipeline stage and process call.

Payload, key, headers, and record ID are omitted from repr output. This is a reduction in accidental
logging exposure, not encryption or a prohibition on explicit access by processors.

## Execution semantics

`StreamPipeline` runs processors sequentially within each batch. A stage may filter records by
returning an empty sequence. Every stage output is bounded and reconstructed as validated
`StreamRecord` values before the next stage runs. Processor exceptions become `ProcessorError`
without the original exception message or cause; cancellation propagates.

`process_many()` bounds in-flight batches, pauses source pulling while that bound is full, and
emits results in source order. This deliberately introduces head-of-line waiting: a slow earlier
batch delays later completed output. On cancellation or a failed source/result, outstanding
processor tasks are cancelled and joined. Processor code must cooperate with cancellation because
Python cannot forcibly stop arbitrary work.

The pipeline and process adapter own no business processor resources. The application manages
processor clients and the Core process lifecycle. `ProcessStreamProcessor.aclose()` prevents new
calls; it does not stop or deactivate Core's process plugin.

## Python and Rust choices

The Python implementation is the required baseline and is appropriate for I/O-heavy or moderate
transformations. `orbit-streams-rust` is an optional full transformation implementation behind the
same typed service contract for workloads where profiling shows Python execution to be material.
RPC serialization, copies, process supervision, and startup are costs; no speedup is guaranteed.
Benchmark representative record size, batch size, concurrency, and failure/restart behavior before
selecting an implementation. A process plugin is a local capability boundary, not a new broker or
general microservice deployment mechanism.

## Failure and delivery boundary

There is no implicit retry, dead-letter behavior, durability, exactly-once processing, cross-batch
ordering, or source acknowledgement. Retry only where replay is safe for processor side effects.
Broker adapters own offsets and acknowledgements and must define how transformation completion
relates to committing a source record. A process call failure may leave processor side effects
ambiguous; a retry can repeat them.

The package is alpha. Python/Rust wire conformance, supported Core versions, local process
failure recovery, and representative performance remain release validation concerns.
