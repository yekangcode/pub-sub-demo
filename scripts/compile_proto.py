#!/usr/bin/env python3
"""Protobuf compiler script."""

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT_DIR / "proto"
OUT_DIR = ROOT_DIR / "src" / "proto"

OUT_DIR.mkdir(parents=True, exist_ok=True)
(OUT_DIR / "__init__.py").touch()

cmd = [
    sys.executable,
    "-m",
    "grpc_tools.protoc",
    f"-I{PROTO_DIR}",
    f"--python_out={OUT_DIR}",
    str(PROTO_DIR / "streaming_event.proto"),
]

print(f"Compiling proto: {' '.join(cmd)}")
subprocess.run(cmd, check=True)
print(f"Proto compiled successfully into {OUT_DIR}")
