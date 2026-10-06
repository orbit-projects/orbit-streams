"""Optional end-to-end checks for a locally built Rust SDK process plugin."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

pytest.importorskip("orbit.plugins.process")

from orbit.health.models import HealthStatus  # noqa: E402
from orbit.plugins.metadata import PluginMetadata  # noqa: E402
from orbit.plugins.process import ProcessPlugin  # noqa: E402

from orbit_streams import ProcessStreamProcessor, StreamRecord  # noqa: E402


@pytest.mark.asyncio
async def test_core_hosts_rust_stream_processor_end_to_end() -> None:
    configured = os.environ.get("ORBIT_STREAMS_RUST_PLUGIN")
    binary = (
        Path(configured)
        if configured
        else (
            Path(__file__).resolve().parents[2]
            / ".."
            / "orbit-streams-rust"
            / "target/debug/examples/append_plugin"
        )
    )
    if not binary.is_file():
        pytest.skip("build orbit-streams-rust --example append_plugin to enable this check")
    plugin = ProcessPlugin(
        PluginMetadata(
            name="orbit-streams-rust",
            version="0.1.0-alpha.1",
            capabilities=frozenset({"orbit.streams"}),
        ),
        (str(binary.resolve()),),
        configuration_json=b'{"suffix":"-rust"}',
        startup_timeout=5,
        command_timeout=3,
        shutdown_timeout=2,
    )
    await plugin.activate()
    try:
        assert (await plugin.health()).status is HealthStatus.HEALTHY
        processor = ProcessStreamProcessor(plugin)
        output = await processor.process((StreamRecord("event-1", b"payload", 42),))
        assert output == (StreamRecord("event-1", b"payload-rust", 42),)
        parallel = await asyncio.gather(
            *(
                processor.process((StreamRecord(f"event-{index}", b"payload", 42),))
                for index in range(16)
            )
        )
        assert [batch[0].value for batch in parallel] == [b"payload-rust"] * 16
        await processor.aclose()
    finally:
        await plugin.deactivate()
