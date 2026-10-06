# Orbit Streams: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-streams` provides bounded, provider-neutral stream records and async batch-transform
contracts for Orbit applications. It is a Python package for Python 3.11–3.14 and has no runtime
dependencies.

```bash
python -m pip install orbit-streams
```

## Delivery and broker boundaries

This package transforms batches. It does not read or persist records, acknowledge offsets, checkpoint
state, retry failures, provide ordering across batches, or guarantee delivery. Broker adapters such
as `orbit-events-kafka`, `orbit-events-rabbitmq`, and `orbit-events-nats` own transport, offset acknowledgement, and
recovery. Applications should acknowledge a source batch only after processing and downstream
handling succeeds. Retrying a processor is the caller's decision because side effects may not be
idempotent.

The implementation is an in-process Python pipeline, not a durable stream engine. It does not claim
end-to-end Rust acceleration: process startup, gRPC, Protobuf, and copies have costs. The separately
installable `orbit-streams-rust` package performs the same bounded batch transformation in a
Core-supervised Rust process for workloads where profiling shows the transformation is a bottleneck.
Both complete implementations use the same typed capability contract, and application wiring
chooses one explicitly. Benchmark real payloads and batch sizes before selecting Rust; this package
makes no blanket performance claim.

Install `orbit-streams[process]` to use the Core process adapter:

```python
from orbit_streams import ProcessStreamProcessor, StreamPipeline

processor = ProcessStreamProcessor(core_managed_process_plugin)
pipeline = StreamPipeline([processor])
```

Configure Core's `ProcessPlugin` with the Rust executable, capability `orbit.streams`, and that
variant's configuration. The adapter translates bounded `StreamRecord` batches to
`orbit.streams.v1.BatchProcessor/Process`. Core owns child lifecycle and the adapter does not.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
