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
"""Regenerate checked-in Python Protobuf message and typing bindings."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto/orbit/streams/v1/stream_processing.proto"
SOURCE = ROOT / "src"
LICENSE = """# Copyright 2026-present Orbit Contributors.
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
"""


def main() -> int:
    """Compile Python messages and type stubs from the public capability schema."""
    with tempfile.TemporaryDirectory(prefix="orbit-streams-proto-") as temp_dir:
        generated = Path(temp_dir)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                "-I",
                str(ROOT / "proto"),
                "--python_out=" + str(generated),
                "--mypy_out=" + str(generated),
                str(PROTO),
            ],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": os.pathsep.join(
                    (str(Path(sys.executable).parent), os.environ.get("PATH", ""))
                ),
            },
            check=True,
        )
        generated_package = generated / "orbit/streams/v1"
        output_package = SOURCE / "orbit_streams/v1"
        output_package.mkdir(parents=True, exist_ok=True)
        for filename in ("stream_processing_pb2.py", "stream_processing_pb2.pyi"):
            shutil.copyfile(generated_package / filename, output_package / filename)
        py_file = output_package / "stream_processing_pb2.py"
        py_file.write_text(
            py_file.read_text(encoding="utf-8").replace(
                "orbit.streams.v1.stream_processing_pb2",
                "orbit_streams.v1.stream_processing_pb2",
            ),
            encoding="utf-8",
        )
    for path in (
        SOURCE / "orbit_streams/v1/stream_processing_pb2.py",
        SOURCE / "orbit_streams/v1/stream_processing_pb2.pyi",
    ):
        text = path.read_text(encoding="utf-8")
        if "Licensed under the Apache License" not in text[:1024]:
            path.write_text(LICENSE + text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
