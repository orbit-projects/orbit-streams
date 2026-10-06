# Orbit Streams: architecture and boundaries

## Responsibility

`orbit-streams` provides bounded, provider-neutral stream records and async batch-transform
contracts for Orbit applications. It is a Python package for Python 3.11–3.14 and has no runtime
dependencies.

```bash
python -m pip install orbit-streams
```

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- No dependencies declared.
- Optional `process` group: `orbit-core[process-plugins]>=0.1.0a1,<0.2`, `protobuf>=7.35.1,<8`.
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`, `build>=1.2,<2`, `grpcio-tools>=1.84,<2`, `mypy-protobuf>=3.6,<4`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_streams/__init__.py`
- `src/orbit_streams/process.py`
- `src/orbit_streams/streaming.py`
- `src/orbit_streams/v1/__init__.py`
- `src/orbit_streams/v1/stream_processing_pb2.py`

## Public contract and scope

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

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
