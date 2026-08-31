"""이중 경로(Dual-Path) 이벤트 발행자 모듈 (Dual-Path Publisher).

[Anthropic 아키텍처 배경]
Claude 서빙 및 멀티모달 추론 환경에서는 수 바이트짜리 텍스트 프롬프트부터 수십 MB에 달하는
고차원 임베딩 텐서 및 이미지 바이너리까지 다양한 크기의 페이로드가 유입됩니다.
Google Cloud Pub/Sub의 단일 메시지 한도는 10MB이므로, 이를 안전하게 처리하기 위해
"이중 경로(Dual-Path) / Claim-Check 패턴"을 구현합니다:
  1. 모든 페이로드는 Zstandard(zstd)로 실시간 압축
  2. 압축 크기 < 8MB: Pub/Sub 메시지 본문에 직접 포함하여 즉시 전송 (Fast Path)
  3. 압축 크기 >= 8MB: Cloud Storage(GCS)에 먼저 업로드 후, 가벼운 URI 포인터만 Pub/Sub으로 발행 (Offload Path)
"""

import hashlib
import os
import socket
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.compression import CompressionManager
from src.gcp_client import GCPClientInterface
from src.proto_gen import streaming_event_pb2

# Anthropic 프로덕션 패턴: Pub/Sub 10MB 한도 도달 전 안전 마진을 둔 8MB 임계값
DEFAULT_OFFLOAD_THRESHOLD_BYTES = 8 * 1024 * 1024


class PublishPath(str, Enum):
    """이벤트가 라우팅된 전송 경로."""

    FAST = "fast"                  # 8MB 미만: Pub/Sub 인라인 직접 전송 (Fast Path)
    GCS_OFFLOAD = "gcs_offload"    # 8MB 이상: GCS 오프로드 및 포인터 전송 (Offload Path)


@dataclass
class PublishResult:
    """단일 스트리밍 이벤트 발행 결과 메트릭 데이터."""

    event_id: str                   # 고유 이벤트 식별자
    message_id: str                 # Pub/Sub 브로커가 반환한 메시지 ID
    path: PublishPath               # 선택된 라우팅 경로 (fast vs gcs_offload)
    uncompressed_bytes: int         # 압축 전 원본 바이트 수
    compressed_bytes: int           # zstd 압축 후 바이트 수
    reduction_percentage: float     # 데이터 절감률 (%)
    payload_uri: str = ""           # GCS 오프로드 시 객체 URI (gs://...)
    schema_fingerprint: str = ""    # 스키마 거버넌스용 SHA-256 핑거프린트
    pod_env_vars: dict[str, str] = field(default_factory=dict)  # GKE Pod 환경 메타데이터
    publish_latency_ms: float = 0.0 # 발행 소요 시간 (밀리초)
    pubsub_wire_bytes: int = 0      # Pub/Sub 브로커 와이어로 실제 전송된 바이트 수 (Dual-Path 절감 측정용)


class DualPathPublisher:
    """Zstandard 압축 및 GCS Claim-Check 오프로드를 결합한 이중 경로 이벤트 발행 클래스."""

    def __init__(
        self,
        client: GCPClientInterface,
        topic_id: str,
        bucket_name: str,
        offload_threshold_bytes: int = DEFAULT_OFFLOAD_THRESHOLD_BYTES,
        compression_level: int = 3,
    ):
        """이중 경로 발행자를 초기화합니다.
        
        Args:
            client: GCP 통신 클라이언트 (MockSandbox 또는 LiveGCPClient)
            topic_id: 목적지 Pub/Sub 토픽 ID
            bucket_name: 대용량 페이로드 저장용 Cloud Storage 버킷명
            offload_threshold_bytes: GCS 오프로드 분기 기준 바이트 수 (기본: 8MB)
            compression_level: Zstandard 압축 레벨 (기본: 3)
        """
        self.client = client
        self.topic_id = topic_id
        self.bucket_name = bucket_name
        self.offload_threshold_bytes = offload_threshold_bytes
        self.compression = CompressionManager(level=compression_level)
        # 현재 컴파일된 Protobuf 스키마의 SHA-256 핑거프린트 계산
        self._schema_fingerprint = self._compute_schema_fingerprint()

    def _compute_schema_fingerprint(self) -> str:
        """Protobuf Descriptor의 직렬화 바이너리를 SHA-256으로 해싱하여 고유 스키마 버전을 생성합니다."""
        descriptor_bytes = streaming_event_pb2.StreamingEvent.DESCRIPTOR.file.serialized_pb
        digest = hashlib.sha256(descriptor_bytes).hexdigest()[:16]
        return f"sha256-{digest}"

    def _collect_pod_env_vars(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """발행자가 실행 중인 GKE Pod 및 호스트 인프라 환경 메타데이터를 수집합니다."""
        env_vars = {
            "node": os.environ.get("HOSTNAME", socket.gethostname()),
            "namespace": os.environ.get("KUBE_NAMESPACE", "default"),
            "job": os.environ.get("JOB_NAME", "streaming-publisher"),
            "csp": "gcp",
        }
        if extra:
            env_vars.update(extra)
        return env_vars

    def publish_event(
        self,
        event_id: str,
        source: str,
        payload: bytes,
        payload_type: str = "text/plain",
        is_corrupted: bool = False,
        extra_pod_env: dict[str, str] | None = None,
        custom_attributes: dict[str, Any] | None = None,
    ) -> PublishResult:
        """단일 이벤트를 Zstd 압축하고 크기에 따라 인라인 또는 GCS 오프로드로 발행합니다."""
        start_time = time.perf_counter()
        uncompressed_size = len(payload)

        # 1. Zstandard로 페이로드 1차 압축 수행
        compressed_payload = self.compression.compress(payload)
        compressed_size = len(compressed_payload)
        reduction = self.compression.reduction_percentage(uncompressed_size, compressed_size)

        pod_env = self._collect_pod_env_vars(extra_pod_env)
        timestamp_ms = int(time.time() * 1000)

        # 2. 압축 크기 기준 Dual-Path 분기 결정
        if compressed_size < self.offload_threshold_bytes:
            # [Fast Path] 8MB 미만: 압축된 페이로드를 Pub/Sub 메시지 본문에 인라인 수납
            path = PublishPath.FAST
            payload_uri = ""
            event_payload = compressed_payload
        else:
            # [Offload Path] 8MB 이상: Cloud Storage에 먼저 업로드
            path = PublishPath.GCS_OFFLOAD
            blob_name = f"payloads/{event_id}.bin"
            payload_uri = self.client.upload_blob(
                bucket_name=self.bucket_name,
                blob_name=blob_name,
                data=compressed_payload,
                content_type="application/zstd",
            )
            # 메시지 본문은 비우고 GCS URI 포인터만 전달 (Claim-Check 패턴)
            event_payload = b""

        # 3. 표준 Protocol Buffers 메시지 생성
        event_proto = streaming_event_pb2.StreamingEvent(
            event_id=event_id,
            source=source,
            payload=event_payload,
            payload_type=payload_type,
            timestamp_ms=timestamp_ms,
            pod_env_vars=pod_env,
            is_corrupted=is_corrupted,
            schema_fingerprint=self._schema_fingerprint,
            payload_uri=payload_uri,
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=compressed_size,
        )

        serialized_data = event_proto.SerializeToString()

        # 4. 다운스트림 필터링 및 디코딩을 위한 Pub/Sub 메시지 속성(Attributes) 구성
        attrs = {
            "path": path.value,
            "event-id": event_id,
            "source": source,
            "payload-type": payload_type,
            "content-encoding": "zstd",
        }
        if payload_uri:
            attrs["payload-uri"] = payload_uri
        if custom_attributes:
            attrs.update({k: str(v) for k, v in custom_attributes.items()})

        # 5. Pub/Sub 토픽으로 발행
        msg_id = self.client.publish(
            topic_id=self.topic_id,
            data=serialized_data,
            attributes=attrs,
        )

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        return PublishResult(
            event_id=event_id,
            message_id=msg_id,
            path=path,
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=compressed_size,
            reduction_percentage=reduction,
            payload_uri=payload_uri,
            schema_fingerprint=self._schema_fingerprint,
            pod_env_vars=pod_env,
            publish_latency_ms=latency_ms,
            pubsub_wire_bytes=len(serialized_data),
        )
