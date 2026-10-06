# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bounded stream records, batch contracts, and an ordered async processing pipeline."""

from __future__ import annotations

import asyncio
import re
from collections import deque
from collections.abc import AsyncIterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, TypeVar, runtime_checkable

_IDENTIFIER = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?$")
_HEADER_NAME = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
MAX_RECORD_BYTES = 256 * 1024
MAX_BATCH_BYTES = 1024 * 1024
MAX_RECORDS_PER_BATCH = 1024
MAX_HEADERS = 16
MAX_HEADER_BYTES = 4096
MAX_PROCESSORS = 32
MAX_CONCURRENCY = 128
_T = TypeVar("_T")


class StreamError(RuntimeError):
    """Raised when a stream batch or pipeline violates its bounded processing contract."""


class ProcessorError(StreamError):
    """Raised when a processor stage fails; provider exception text is intentionally omitted."""


@dataclass(frozen=True, slots=True)
class StreamRecord:
    """Immutable provider-neutral record with bounded, redacted payload and key values."""

    record_id: str = field(repr=False)
    value: bytes = field(repr=False)
    timestamp_ms: int
    key: bytes | None = field(default=None, repr=False)
    content_type: str = "application/octet-stream"
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        """Detach caller buffers and validate each field before the record enters a batch."""
        if not isinstance(self.record_id, str) or _IDENTIFIER.fullmatch(self.record_id) is None:
            raise ValueError("record_id must be a bounded ASCII identifier.")
        if not isinstance(self.value, bytes) or len(self.value) > MAX_RECORD_BYTES:
            raise ValueError("record value must be bytes no larger than 256 KiB.")
        if self.key is not None and (
            not isinstance(self.key, bytes) or len(self.key) > MAX_HEADER_BYTES
        ):
            raise ValueError("record key must be bytes no larger than 4 KiB.")
        if (
            isinstance(self.timestamp_ms, bool)
            or not isinstance(self.timestamp_ms, int)
            or not 0 <= self.timestamp_ms <= 2**63 - 1
        ):
            raise ValueError("timestamp_ms must be a non-negative signed 64-bit integer.")
        if (
            not isinstance(self.content_type, str)
            or not self.content_type
            or len(self.content_type) > 128
            or any(ord(char) < 33 or ord(char) > 126 for char in self.content_type)
        ):
            raise ValueError("content_type must be bounded printable ASCII.")
        object.__setattr__(self, "value", bytes(self.value))
        if self.key is not None:
            object.__setattr__(self, "key", bytes(self.key))
        object.__setattr__(self, "headers", _freeze_headers(self.headers))


@dataclass(frozen=True, slots=True)
class StreamBatch:
    """Immutable, ordered records constrained by count and aggregate payload size."""

    records: tuple[StreamRecord, ...]
    max_records: int = MAX_RECORDS_PER_BATCH
    max_bytes: int = MAX_BATCH_BYTES

    def __post_init__(self) -> None:
        """Copy the record sequence and enforce bounds used by every pipeline stage."""
        _validate_bound(self.max_records, "max_records", MAX_RECORDS_PER_BATCH)
        _validate_bound(self.max_bytes, "max_bytes", MAX_BATCH_BYTES)
        if isinstance(self.records, (str, bytes)) or not isinstance(self.records, Sequence):
            raise TypeError("records must be a sequence of StreamRecord values.")
        records = _copy_bounded_sequence(self.records, self.max_records, "max_records")
        if any(not isinstance(record, StreamRecord) for record in records):
            raise TypeError("batch records must be StreamRecord values.")
        size = sum(_record_size(record) for record in records)
        if size > self.max_bytes:
            raise ValueError("batch exceeds max_bytes.")
        object.__setattr__(self, "records", records)

    @property
    def byte_size(self) -> int:
        """Return the bounded serialized-field size, excluding Python object overhead."""
        return sum(_record_size(record) for record in self.records)


@runtime_checkable
class BatchProcessor(Protocol):
    """Async transformation contract; it does not own source acknowledgement or durability."""

    async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
        """Transform or filter one bounded ordered batch; preserve cancellation semantics."""


class StreamPipeline:
    """Run ordered, bounded transformation stages and backpressure concurrent batch sources.

    The pipeline owns no broker, task queue, checkpoint, retry, transaction, or processor resource.
    It awaits each stage and preserves batch order. Callers acknowledge or commit source records
    only after successfully handling the returned batch; replay safety remains application policy.
    """

    def __init__(
        self,
        processors: Sequence[BatchProcessor],
        *,
        max_records: int = MAX_RECORDS_PER_BATCH,
        max_bytes: int = MAX_BATCH_BYTES,
    ) -> None:
        """Validate and freeze a bounded sequence of transformation stages."""
        _validate_bound(max_records, "max_records", MAX_RECORDS_PER_BATCH)
        _validate_bound(max_bytes, "max_bytes", MAX_BATCH_BYTES)
        if isinstance(processors, (str, bytes)) or not isinstance(processors, Sequence):
            raise TypeError("processors must be a sequence of BatchProcessor values.")
        stages = _copy_bounded_sequence(processors, MAX_PROCESSORS, "pipeline processors")
        if any(not isinstance(processor, BatchProcessor) for processor in stages):
            raise TypeError("each processor must provide async process(records).")
        self._processors = stages
        self._max_records = max_records
        self._max_bytes = max_bytes

    async def process(self, batch: StreamBatch) -> StreamBatch:
        """Apply each stage in order, rejecting oversized outputs and sanitizing failures."""
        if not isinstance(batch, StreamBatch):
            raise TypeError("batch must be a StreamBatch.")
        result = StreamBatch(batch.records, self._max_records, self._max_bytes)
        for index, processor in enumerate(self._processors, start=1):
            try:
                output = await processor.process(result.records)
                if isinstance(output, (str, bytes)) or not isinstance(output, Sequence):
                    raise TypeError("processor output must be a record sequence")
                bounded_output = _copy_bounded_sequence(
                    output, self._max_records, "processor output"
                )
                result = StreamBatch(bounded_output, self._max_records, self._max_bytes)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise ProcessorError(f"Stream processor stage {index} failed.") from None
        return result

    async def process_many(
        self,
        source: AsyncIterable[StreamBatch],
        *,
        max_concurrency: int = 8,
    ) -> AsyncIterable[StreamBatch]:
        """Process a source with bounded in-flight batches and source-order output.

        Pulling pauses at ``max_concurrency`` until the oldest batch completes. On failure or
        cancellation, outstanding tasks are cancelled and joined. Processors must cooperate with
        cancellation; this method never retries or acknowledges input.
        """
        _validate_bound(max_concurrency, "max_concurrency", MAX_CONCURRENCY)
        pending: deque[asyncio.Task[StreamBatch]] = deque()
        try:
            async for batch in source:
                if not isinstance(batch, StreamBatch):
                    raise TypeError("source values must be StreamBatch values.")
                pending.append(asyncio.create_task(self.process(batch)))
                if len(pending) >= max_concurrency:
                    yield await pending.popleft()
            while pending:
                yield await pending.popleft()
        except BaseException:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise


def _freeze_headers(value: object) -> Mapping[str, str]:
    """Copy string headers into a read-only map with stable name and byte limits."""
    if not isinstance(value, Mapping):
        raise TypeError("headers must be a mapping of string names and values.")
    result: dict[str, str] = {}
    size = 0
    for index, (name, content) in enumerate(value.items(), start=1):
        if index > MAX_HEADERS:
            raise ValueError("record exceeds the 16 header limit.")
        if not isinstance(name, str) or _HEADER_NAME.fullmatch(name) is None:
            raise ValueError("header names must be bounded lowercase ASCII identifiers.")
        if (
            not isinstance(content, str)
            or len(content.encode("utf-8")) > 256
            or any(ord(char) < 32 or ord(char) == 127 for char in content)
        ):
            raise ValueError("header values must be bounded text without control characters.")
        size += len(name.encode("ascii")) + len(content.encode("utf-8"))
        if size > MAX_HEADER_BYTES:
            raise ValueError("record headers exceed 4 KiB.")
        result[name] = content
    return MappingProxyType(result)


def _record_size(record: StreamRecord) -> int:
    """Count user-controlled fields for bounded batch admission."""
    key_size = 0 if record.key is None else len(record.key)
    header_size = sum(
        len(name) + len(value.encode("utf-8")) for name, value in record.headers.items()
    )
    return (
        len(record.value)
        + key_size
        + header_size
        + len(record.record_id)
        + len(record.content_type)
    )


def _copy_bounded_sequence(value: Sequence[_T], maximum: int, label: str) -> tuple[_T, ...]:
    """Copy at most maximum plus one elements without trusting a custom Sequence length."""
    result: list[_T] = []
    iterator = iter(value)
    for _ in range(maximum + 1):
        try:
            result.append(next(iterator))
        except StopIteration:
            break
    if len(result) > maximum:
        raise ValueError(f"{label} exceeds its configured limit.")
    return tuple(result)


def _validate_bound(value: object, name: str, maximum: int) -> None:
    """Require a positive integer below the package's hard upper limit."""
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}.")
