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

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence

import pytest

from orbit_streams import (
    ProcessorError,
    ProcessStreamProcessor,
    StreamBatch,
    StreamPipeline,
    StreamRecord,
)


def record(record_id: str, value: bytes | None = None) -> StreamRecord:
    return StreamRecord(
        record_id=record_id,
        value=record_id.encode() if value is None else value,
        timestamp_ms=1,
    )


class Suffix:
    def __init__(self, suffix: bytes) -> None:
        self.suffix = suffix

    async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
        return tuple(
            StreamRecord(item.record_id, item.value + self.suffix, item.timestamp_ms)
            for item in records
        )


class Keep:
    async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
        return tuple(item for item in records if item.record_id != "drop")


@pytest.mark.asyncio
async def test_pipeline_applies_stages_in_order_and_allows_filtering() -> None:
    pipeline = StreamPipeline([Suffix(b"-one"), Keep(), Suffix(b"-two")])

    output = await pipeline.process(StreamBatch((record("a"), record("drop"))))

    assert [(item.record_id, item.value) for item in output.records] == [("a", b"a-one-two")]


def test_record_snapshots_headers_and_hides_sensitive_fields_from_repr() -> None:
    headers = {"tenant": "private-tenant"}
    item = StreamRecord(
        record_id="private-id",
        value=b"secret payload",
        timestamp_ms=10,
        key=b"private-key",
        headers=headers,
    )
    headers["tenant"] = "changed"

    assert item.headers["tenant"] == "private-tenant"
    assert "secret" not in repr(item)
    assert "private" not in repr(item)
    with pytest.raises(TypeError):
        item.headers["tenant"] = "mutable"  # type: ignore[index]


def test_record_and_batch_limits_are_enforced() -> None:
    with pytest.raises(ValueError, match="256 KiB"):
        record("large", b"x" * (256 * 1024 + 1))
    with pytest.raises(ValueError, match="max_records"):
        StreamBatch((record("one"), record("two")), max_records=1)
    with pytest.raises(ValueError, match="max_bytes"):
        StreamBatch((record("large", b"x" * (256 * 1024)),) * 4, max_bytes=1024)
    with pytest.raises(ValueError, match="header"):
        StreamRecord("a", b"", 0, headers={"Bad": "value"})


@pytest.mark.asyncio
async def test_process_many_bounds_concurrency_and_preserves_source_order() -> None:
    class Track:
        active = 0
        max_active = 0

        async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                if records[0].record_id == "0":
                    await asyncio.sleep(0.02)
                else:
                    await asyncio.sleep(0)
                return records
            finally:
                self.active -= 1

    processor = Track()
    pipeline = StreamPipeline([processor])

    async def source() -> AsyncIterator[StreamBatch]:
        for index in range(6):
            yield StreamBatch((record(str(index)),))

    results = [item async for item in pipeline.process_many(source(), max_concurrency=2)]

    assert [item.records[0].record_id for item in results] == [str(index) for index in range(6)]
    assert processor.max_active == 2


@pytest.mark.asyncio
async def test_pipeline_sanitizes_processor_errors_without_retrying() -> None:
    class Failure:
        calls = 0

        async def process(self, _records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
            self.calls += 1
            raise RuntimeError("provider-secret-value")

    processor = Failure()
    pipeline = StreamPipeline([processor])
    with pytest.raises(ProcessorError, match="stage 1") as raised:
        await pipeline.process(StreamBatch((record("a"),)))
    assert "provider-secret" not in str(raised.value)
    assert processor.calls == 1


@pytest.mark.asyncio
async def test_process_many_cancels_pending_processors() -> None:
    class Waiting:
        started = 0
        cancelled = 0
        both_started = asyncio.Event()

        async def process(self, records: tuple[StreamRecord, ...]) -> Sequence[StreamRecord]:
            self.started += 1
            if self.started == 2:
                self.both_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled += 1

    processor = Waiting()
    pipeline = StreamPipeline([processor])

    async def source() -> AsyncIterator[StreamBatch]:
        yield StreamBatch((record("a"),))
        yield StreamBatch((record("b"),))

    async def consume() -> None:
        async for _ in pipeline.process_many(source(), max_concurrency=2):
            pass

    task = asyncio.create_task(consume())
    await asyncio.wait_for(processor.both_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert processor.cancelled == 2


def test_pipeline_rejects_unbounded_processor_configuration() -> None:
    with pytest.raises(ValueError, match="configured limit"):
        StreamPipeline([Suffix(b"x")] * 33)


def test_record_constructor_rejects_invalid_types_and_identifier() -> None:
    with pytest.raises(ValueError, match="record_id"):
        record("bad/id")
    with pytest.raises(TypeError):
        StreamBatch((object(),))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_process_stream_processor_uses_typed_contract_and_validates_response() -> None:
    class EchoProcess:
        async def invoke(
            self,
            method: str,
            request: object,
            response: object,
            *,
            timeout: float | None = None,
        ) -> object:
            from orbit_streams.v1 import stream_processing_pb2

            assert method == "/orbit.streams.v1.BatchProcessor/Process"
            assert timeout == 2
            assert isinstance(request, stream_processing_pb2.StreamBatch)
            assert isinstance(response, stream_processing_pb2.StreamBatch)
            response.CopyFrom(request)
            response.records[0].value = b"rust-result"
            return response

    processor = ProcessStreamProcessor(EchoProcess(), request_timeout=2)
    result = await processor.process((record("a"),))
    assert result == (record("a", b"rust-result"),)


@pytest.mark.asyncio
async def test_process_stream_processor_rejects_unbounded_response() -> None:
    class InvalidProcess:
        async def invoke(
            self,
            _method: str,
            _request: object,
            response: object,
            *,
            timeout: float | None = None,
        ) -> object:
            from orbit_streams.v1 import stream_processing_pb2

            assert isinstance(response, stream_processing_pb2.StreamBatch)
            response.max_records = 1024
            response.max_bytes = 1024 * 1024
            response.records.add(record_id="bad/id", value=b"x", timestamp_ms=1)
            return response

    processor = ProcessStreamProcessor(InvalidProcess())
    with pytest.raises(RuntimeError, match="invalid batch"):
        await processor.process((record("a"),))


@pytest.mark.asyncio
async def test_process_stream_processor_rejects_protobuf_overhead_before_rpc() -> None:
    class NeverInvoked:
        called = False

        async def invoke(
            self,
            _method: str,
            _request: object,
            _response: object,
            *,
            timeout: float | None = None,
        ) -> object:
            self.called = True
            raise AssertionError("oversized protobuf request must fail before RPC")

    records = tuple(
        StreamRecord(
            record_id=chr(ord("a") + index),
            value=b"x" * 262_142,
            timestamp_ms=0,
            content_type="x",
        )
        for index in range(4)
    )
    process = NeverInvoked()
    processor = ProcessStreamProcessor(process)
    with pytest.raises(RuntimeError, match="wire limit"):
        await processor.process(records)
    assert not process.called
