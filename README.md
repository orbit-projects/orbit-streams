# Orbit Streams

`orbit-streams` provides bounded, provider-neutral stream records and async batch-transform
contracts for Orbit applications. It is a Python package for Python 3.11–3.14 and has no runtime
dependencies.

```bash
python -m pip install orbit-streams
```

## Processing contract

`StreamRecord` snapshots immutable bytes and bounded headers. `StreamBatch` limits each batch to at
most 1,024 records and 1 MiB of user-controlled fields. A record value is limited to 256 KiB; a key
to 4 KiB; and headers to 16 entries and 4 KiB total. Payloads, keys, headers, and record identifiers
are omitted from record representations.

Implement `BatchProcessor.process(records)` as an async transformation. A `StreamPipeline` runs up
to 32 processors in order and validates each intermediate output against the same bounds. Filters
may return an empty sequence. Failures become `ProcessorError` without exposing provider exception
text; task cancellation propagates.

`process_many()` limits in-flight batches to a caller-selected concurrency between 1 and 128. It
preserves source order, stops pulling while the bound is full, and cancels and joins pending work on
failure or cancellation. Processors must cooperate with task cancellation. The pipeline owns no
processor resources; application lifecycle code owns any resources used by processor implementations.

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

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
ruff format --check .
mypy
python -m build
```

Tests use deterministic processors and do not require a broker. Live broker delivery, restart
recovery, deployment, Python/Rust host conformance, and sustained-load behavior are not covered by
the in-process tests. This package is pre-alpha, not a production certification.

Licensed under Apache-2.0.

