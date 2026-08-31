"""Zstandard 실시간 페이로드 압축/압축해제 모듈 (Zstandard Compression Layer).

[Anthropic 아키텍처 배경]
Anthropic은 Claude 서빙 및 모델 학습 원격 측정(Telemetry) 이벤트 스트리밍 시,
네트워크 대역폭을 60%~80% 절감하고 브로커 전송 비용을 최소화하기 위해
실시간 처리 속도가 매우 뛰어난 Zstandard(zstd) 알고리즘을 기본 압축 레이어로 채택했습니다.
"""

from typing import Any

import zstandard as zstd

# Zstandard 표준 프레임 매직 넘버: 0xFD2FB528 (Little-Endian 바이트 시퀀스: 0x28, 0xB5, 0x2F, 0xFD)
# 수신된 바이너리 데이터가 zstd로 압축된 정상 프레임인지 판별하는 데 사용됩니다.
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


class CompressionManager:
    """Zstandard를 활용하여 페이로드를 압축 및 압축 해제하고 메타데이터를 관리하는 클래스."""

    def __init__(self, level: int = 3):
        """지정된 압축 레벨(1~22)로 Zstandard 컨텍스트를 초기화합니다.
        
        기본 레벨 3은 실시간 스트리밍에 최적화된 압축 속도(초당 수백 MB)와 높은 압축률 간의 최적 균형점입니다.
        """
        self.level = level
        # 압축 컨텍스트 (ZstdCompressor)
        self._cctx = zstd.ZstdCompressor(level=self.level)
        # 압축 해제 컨텍스트 (ZstdDecompressor)
        self._dctx = zstd.ZstdDecompressor()

    def compress(self, data: bytes) -> bytes:
        """원시 바이트 데이터를 Zstandard 알고리즘으로 압축합니다."""
        if not isinstance(data, bytes):
            raise TypeError("압축 대상 데이터는 bytes 형식이어야 합니다.")
        return self._cctx.compress(data)

    def decompress(self, compressed_data: bytes) -> bytes:
        """Zstandard로 압축된 바이너리 데이터를 원본 바이트로 복원합니다."""
        if not isinstance(compressed_data, bytes):
            raise TypeError("압축 해제 대상 데이터는 bytes 형식이어야 합니다.")
        return self._dctx.decompress(compressed_data)

    def is_compressed(self, data: bytes) -> bool:
        """데이터의 시작 4바이트가 Zstandard 매직 넘버와 일치하는지 검사하여 압축 여부를 판별합니다."""
        return isinstance(data, bytes) and data.startswith(ZSTD_MAGIC)

    @staticmethod
    def reduction_percentage(original_size: int, compressed_size: int) -> float:
        """원본 크기 대비 절감된 용량 비율(0.0% ~ 100.0%)을 계산합니다."""
        if original_size <= 0:
            return 0.0
        reduced = max(0, original_size - compressed_size)
        return (reduced / original_size) * 100.0

    @staticmethod
    def enrich_attributes_with_encoding(
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Pub/Sub 메시지 속성(Attributes)에 표준 인코딩 헤더('content-encoding': 'zstd')를 주입합니다.
        
        이를 통해 다운스트림 컨슈머가 페이로드 디코딩 방식을 즉시 식별할 수 있습니다.
        """
        result = {k: str(v) for k, v in (attributes or {}).items()}
        result["content-encoding"] = "zstd"
        return result

    @staticmethod
    def has_zstd_encoding(attributes: dict[str, str] | None = None) -> bool:
        """메시지 속성에 'content-encoding': 'zstd'가 지정되어 있는지 확인합니다."""
        if not attributes:
            return False
        return attributes.get("content-encoding") == "zstd"
