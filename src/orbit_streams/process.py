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
"""Bridge bounded stream batches to a Core-supervised process plugin."""

from __future__ import annotations

import asyncio
from typing import Protocol

from orbit_streams.streaming import MAX_BATCH_BYTES, StreamBatch, StreamRecord


class _ProcessInvoker(Protocol):
    """Minimal operation exposed by Core's process-plugin host."""

    async def invoke(
        self,
        method: str,
        request: object,
        response: object,
        *,
        timeout: float | None = None,
    ) -> object:
        """Invoke a typed capability call on the active process plugin."""


class ProcessStreamProcessor:
    """Use a Rust or other process implementation as a regular async batch processor.

    Core owns child startup, authentication, configuration, health, and shutdown. This adapter
    translates the provider-neutral Python records to the ``orbit.streams.v1`` Protobuf schema.
    """

    def __init__(self, process: _ProcessInvoker, *, request_timeout: float = 5.0) -> None:
        """Bind an active Core process and apply a deadline to each batch call."""
        if isinstance(request_timeout, bool) or not isinstance(request_timeout, (int, float)):
            raise TypeError("request_timeout must be a number.")
        if not 0.05 <= request_timeout <= 60.0:
            raise ValueError("request_timeout must be between 0.05 and 60 seconds.")
        self._process = process
        self._request_timeout = float(request_timeout)
        self._closed = False

    async def process(self, records: tuple[StreamRecord, ...]) -> tuple[StreamRecord, ...]:
        """Transform one bounded batch and reject malformed or oversized plugin responses."""
        from orbit_streams.v1 import stream_processing_pb2

        if self._closed:
            raise RuntimeError("Process stream adapter is closed.")
        batch = StreamBatch(records)
        request = stream_processing_pb2.StreamBatch(
            max_records=batch.max_records,
            max_bytes=batch.max_bytes,
        )
        for record in batch.records:
            item = request.records.add(
                record_id=record.record_id,
                value=record.value,
                timestamp_ms=record.timestamp_ms,
                content_type=record.content_type,
                headers=dict(record.headers),
            )
            if record.key is not None:
                item.key = record.key
        if request.ByteSize() > MAX_BATCH_BYTES:
            raise RuntimeError("Stream process request exceeds the 1 MiB wire limit.")
        response = stream_processing_pb2.StreamBatch()
        try:
            await self._process.invoke(
                "/orbit.streams.v1.BatchProcessor/Process",
                request,
                response,
                timeout=self._request_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RuntimeError("Stream process plugin call failed.") from None
        try:
            if response.ByteSize() > MAX_BATCH_BYTES:
                raise ValueError("response exceeds the 1 MiB wire limit")
            if response.max_records != batch.max_records or response.max_bytes != batch.max_bytes:
                raise ValueError("response limits do not match request")
            if len(response.records) > batch.max_records:
                raise ValueError("response record count exceeds limit")
            result = tuple(
                StreamRecord(
                    record_id=item.record_id,
                    value=bytes(item.value),
                    timestamp_ms=item.timestamp_ms,
                    key=bytes(item.key) if item.HasField("key") else None,
                    content_type=item.content_type,
                    headers=dict(item.headers),
                )
                for item in response.records
            )
            return StreamBatch(result, batch.max_records, batch.max_bytes).records
        except (TypeError, ValueError):
            raise RuntimeError("Stream process plugin returned an invalid batch.") from None

    async def aclose(self) -> None:
        """Stop new calls without taking ownership of Core's process host lifecycle."""
        self._closed = True
