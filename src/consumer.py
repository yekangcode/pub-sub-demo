"""이중 경로(Dual-Path) 이벤트 수신 및 복원 컨슈머 모듈 (Dual-Path Consumer).

[Anthropic 아키텍처 배경]
이중 경로 패턴의 핵심 가치는 "다운스트림 투명성(Downstream Transparency)"입니다.
이벤트를 소비하는 수백 개의 마이크로서비스나 분석 파이프라인은 데이터가 8MB 미만이어서
Pub/Sub 인라인으로 전송되었는지, 혹은 8MB 이상이어서 GCS에 오프로드되었는지 알 필요가 없습니다.
DualPathConsumer는 메시지의 payload_uri 유무를 확인하여, 인라인이면 즉시 zstd 압축을 해제하고
GCS 오프로드이면 스토리지에서 바이너리를 가져와 동일한 원본 객체로 투명하게 복원합니다.
"""

import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from src.compression import CompressionManager
from src.gcp_client import GCPClientInterface
from src.proto_gen import streaming_event_pb2
from src.publisher import PublishPath


@dataclass
class ReconstitutedEvent:
    """인라인 또는 GCS 오프로드로부터 완전히 복원된 스트리밍 이벤트 객체."""

    event_id: str                   # 이벤트 식별자
    source: str                     # 발행 소스 (예: serving-claude)
    payload: bytes                  # 완전히 복원 및 압축 해제된 원본 페이로드 바이트
    payload_type: str               # MIME 타입 (예: text/plain, application/octet-stream)
    path: PublishPath               # 수신 경로 (fast vs gcs_offload)
    payload_uri: str                # GCS URI (오프로드인 경우)
    schema_fingerprint: str         # 스키마 핑거프린트
    pod_env_vars: dict[str, str] = field(default_factory=dict)  # 발행 Pod 메타데이터
    processing_latency_ms: float = 0.0                          # 복원 처리 소요 시간 (밀리초)


class DualPathConsumer:
    """Fast Path(인라인 압축 바이트) 및 Offload Path(GCS 다운로드 + 압축해제)를 투명하게 복원하는 컨슈머 클래스."""

    def __init__(self, client: GCPClientInterface):
        """컨슈머를 초기화합니다.
        
        Args:
            client: GCS 객체 다운로드를 위한 GCP 클라이언트
        """
        self.client = client
        self.compression = CompressionManager()

    def _parse_gs_uri(self, uri: str) -> tuple[str, str]:
        """gs://{bucket}/{blob_name} 포맷의 GCS URI를 (bucket, blob_name) 튜플로 파싱합니다."""
        parsed = urlparse(uri)
        if parsed.scheme != "gs":
            raise ValueError(f"유효하지 않은 GCS URI 스킴입니다: {uri}")
        bucket = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        return bucket, blob_name

    def consume_message(
        self, data: bytes, attributes: dict[str, Any] | None = None
    ) -> ReconstitutedEvent:
        """수신된 Pub/Sub 메시지 바이너리를 역직렬화하고 페이로드를 원본 상태로 복원합니다."""
        start_time = time.perf_counter()

        # 1. Protocol Buffers 메시지 역직렬화
        event_proto = streaming_event_pb2.StreamingEvent()
        event_proto.ParseFromString(data)

        # 2. 의도적 손상 / 포이즌 필 검증 (DLQ 테스트용)
        # 악성 메시지가 유입되면 예외를 발생시켜 DLQ 재시도 로직으로 전달합니다.
        if event_proto.is_corrupted:
            raise ValueError(f"Corrupted event detected: {event_proto.event_id}")

        # 3. 경로 판별 및 페이로드 복원 (투명한 복원 메커니즘)
        if event_proto.payload_uri:
            # [Offload Path] GCS에 저장된 경우: Cloud Storage에서 압축 바이너리 다운로드 후 zstd 해제
            path = PublishPath.GCS_OFFLOAD
            bucket, blob_name = self._parse_gs_uri(event_proto.payload_uri)
            compressed_blob = self.client.download_blob(bucket_name=bucket, blob_name=blob_name)
            decompressed_payload = self.compression.decompress(compressed_blob)
        else:
            # [Fast Path] 인라인인 경우: 본문의 바이트를 즉시 zstd 압축 해제
            path = PublishPath.FAST
            decompressed_payload = self.compression.decompress(event_proto.payload)

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        return ReconstitutedEvent(
            event_id=event_proto.event_id,
            source=event_proto.source,
            payload=decompressed_payload,
            payload_type=event_proto.payload_type,
            path=path,
            payload_uri=event_proto.payload_uri,
            schema_fingerprint=event_proto.schema_fingerprint,
            pod_env_vars=dict(event_proto.pod_env_vars),
            processing_latency_ms=latency_ms,
        )
