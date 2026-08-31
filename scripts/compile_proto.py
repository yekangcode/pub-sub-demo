#!/usr/bin/env python3
"""Protocol Buffers 스키마 컴파일 자동화 스크립트.

[Anthropic 아키텍처 배경]
Anthropic의 "Proto-First 셀프서비스 플랫폼" 철학에 따라, 모든 스트리밍 이벤트는
JSON과 같은 비정형 텍스트 대신 정밀한 바이너리 스키마인 Protocol Buffers(`.proto`)로 정의됩니다.
본 스크립트는 `proto/streaming_event.proto`를 파싱하여 Python용 데이터 클래스(`src/proto/streaming_event_pb2.py`)를
자동 컴파일 및 코드 생성합니다.
"""

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT_DIR / "proto"
OUT_DIR = ROOT_DIR / "src" / "proto"

# 출력 대상 디렉토리 생성 및 패키지 초기화 파일(__init__.py) 보장
OUT_DIR.mkdir(parents=True, exist_ok=True)
(OUT_DIR / "__init__.py").touch()

# grpc_tools.protoc 명령어를 통한 Python 직렬화 클래스 빌드
cmd = [
    sys.executable,
    "-m",
    "grpc_tools.protoc",
    f"-I{PROTO_DIR}",
    f"--python_out={OUT_DIR}",
    str(PROTO_DIR / "streaming_event.proto"),
]

print(f"Protocol Buffers 스키마 컴파일 중: {' '.join(cmd)}")
subprocess.run(cmd, check=True)
print(f"✓ 스키마 컴파일 완료: {OUT_DIR / 'streaming_event_pb2.py'}")
