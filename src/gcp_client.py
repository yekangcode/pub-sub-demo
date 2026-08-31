"""Google Cloud 클라이언트 추상화 계층 모듈 (GCP Client Abstraction Layer).

[설계 의도 및 아키텍처 배경]
이 모듈은 의존성 역전 원칙(Dependency Inversion Principle)을 적용하여,
비즈니스 로직(이중 경로 분기, Zstd 압축, DLQ 격리)이 하부 인프라의 물리적 통신 방식에 의존하지 않도록 분리합니다:
  1. GCPClientInterface: Pub/Sub 발행 및 Cloud Storage 업로드/다운로드 공통 규격 정의
  2. MockGCPClient: 로컬/오프라인 데모 및 CI/CD 단위 테스트를 위한 인메모리(In-Memory) 구현체
  3. LiveGCPClient: 실제 Google Cloud (pub-sub-kamo) 엔드포인트와 gRPC/HTTP로 통신하는 라이브 구현체
  4. GCPClientFactory: 싱글톤 패턴으로 두 모드 간 원클릭 런타임 전환 지원
"""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar


class GCPMode(str, Enum):
    """GCP 작업 실행 모드."""

    LIVE = "live"  # 실제 Google Cloud 프로젝트 연동 모드
    MOCK = "mock"  # 로컬 메모리 기반 모의 샌드박스 모드


@dataclass
class PublishedMessage:
    """Pub/Sub 토픽에 발행 및 기록된 메시지 모델."""

    message_id: str                          # 고유 메시지 ID
    data: bytes                              # 직렬화된 메시지 페이로드 바이너리
    attributes: dict[str, str] = field(default_factory=dict)  # 메시지 속성 메타데이터
    publish_time: float = 0.0                # 발행 타임스탬프


class GCPClientInterface(ABC):
    """Google Cloud Pub/Sub 및 Cloud Storage 통신을 위한 추상 인터페이스."""

    def __init__(self, project_id: str, mode: GCPMode):
        self.project_id = project_id
        self.mode = mode

    @abstractmethod
    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
        """Pub/Sub 토픽으로 메시지를 발행하고 생성된 message_id를 반환합니다."""

    @abstractmethod
    def upload_blob(
        self,
        bucket_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """바이너리 객체를 Cloud Storage 버킷에 업로드하고 gs:// URI를 반환합니다."""

    @abstractmethod
    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        """Cloud Storage 버킷에서 바이너리 객체 콘텐츠를 다운로드합니다."""

    @abstractmethod
    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """Cloud Storage 버킷에서 특정 객체를 삭제합니다."""


class MockGCPClient(GCPClientInterface):
    """로컬 실행, 오프라인 미팅, 초고속 단위 테스트를 위한 인메모리(In-Memory) 샌드박스 클라이언트.
    
    실제 인터넷 망이나 GCP 계정 권한 없이도 Pub/Sub 토픽 큐잉 및 GCS 객체 저장을
    완벽히 시뮬레이션합니다.
    """

    def __init__(self, project_id: str):
        super().__init__(project_id=project_id, mode=GCPMode.MOCK)
        # 토픽별 발행된 메시지 큐 (메모리 딕셔너리)
        self.topics: dict[str, list[PublishedMessage]] = {}
        # 버킷별 저장된 바이너리 객체 저장소 (메모리 딕셔너리)
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.subscribers: dict[str, list] = {}

    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
        """인메모리 토픽 큐에 메시지를 추가합니다."""
        msg_id = f"mock-msg-{uuid.uuid4().hex[:12]}"
        msg = PublishedMessage(
            message_id=msg_id,
            data=data,
            attributes=dict(attributes or {}),
        )
        if topic_id not in self.topics:
            self.topics[topic_id] = []
        self.topics[topic_id].append(msg)
        return msg_id

    def get_published_messages(self, topic_id: str) -> list[PublishedMessage]:
        """특정 토픽에 발행된 메시지 목록을 조회합니다 (모의 샌드박스 유틸리티)."""
        return list(self.topics.get(topic_id, []))

    def clear_topic(self, topic_id: str) -> None:
        """토픽에 쌓인 메시지 큐를 초기화합니다."""
        self.topics[topic_id] = []

    def upload_blob(
        self,
        bucket_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """인메모리 버킷에 바이너리 객체를 저장합니다."""
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = {}
        self.buckets[bucket_name][blob_name] = data
        return f"gs://{bucket_name}/{blob_name}"

    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        """인메모리 버킷에서 바이너리 객체를 조회합니다."""
        if bucket_name not in self.buckets or blob_name not in self.buckets[bucket_name]:
            raise KeyError(f"Blob gs://{bucket_name}/{blob_name}을 찾을 수 없습니다.")
        return self.buckets[bucket_name][blob_name]

    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """인메모리 버킷에서 바이너리 객체를 삭제합니다."""
        if bucket_name in self.buckets and blob_name in self.buckets[bucket_name]:
            del self.buckets[bucket_name][blob_name]


class LiveGCPClient(GCPClientInterface):
    """Google Cloud 공식 Python SDK(pubsub_v1, storage)를 사용하는 실제 클라우드 통신 클라이언트."""

    def __init__(self, project_id: str, batch_settings: Any | None = None):
        """LiveGCPClient를 초기화합니다.
        
        Args:
            project_id: 대상 GCP 프로젝트 ID
            batch_settings: 1KB 최소 과금 우회 및 처리량 극대화를 위한 pubsub_v1.types.BatchSettings 인스턴스
        """
        super().__init__(project_id=project_id, mode=GCPMode.LIVE)
        self._publisher = None
        self._storage_client = None
        self.batch_settings = batch_settings

    @property
    def publisher(self):
        """필요 시점에 Google Cloud Pub/Sub PublisherClient를 지연 로딩(Lazy initialization)합니다.
        
        1KB 최소 과금 크기 우회를 위한 BatchSettings(max_messages, max_bytes, max_latency)가
        지정된 경우 클라이언트에 주입합니다.
        """
        if self._publisher is None:
            from google.cloud import pubsub_v1

            kwargs = {}
            if self.batch_settings is not None:
                kwargs["batch_settings"] = self.batch_settings
            self._publisher = pubsub_v1.PublisherClient(**kwargs)
        return self._publisher

    @property
    def storage(self):
        """필요 시점에 Google Cloud Storage Client를 지연 로딩합니다."""
        if self._storage_client is None:
            from google.cloud import storage

            self._storage_client = storage.Client(project=self.project_id)
        return self._storage_client

    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
        """실제 Google Cloud Pub/Sub 토픽으로 gRPC 호출을 통해 메시지를 발행합니다."""
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        future = self.publisher.publish(topic_path, data=data, **(attributes or {}))
        return future.result()

    def upload_blob(
        self,
        bucket_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """실제 Cloud Storage 버킷에 바이너리 데이터를 업로드합니다."""
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{bucket_name}/{blob_name}"

    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        """실제 Cloud Storage 버킷에서 바이너리 데이터를 바이트로 다운로드합니다."""
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()

    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """실제 Cloud Storage 버킷에서 객체를 삭제합니다."""
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()


class GCPClientFactory:
    """클라이언트 인스턴스 생성을 관리하는 팩토리 클래스 (싱글톤 인스턴스 캐싱)."""

    _instances: ClassVar[dict[tuple[GCPMode, str], GCPClientInterface]] = {}

    @classmethod
    def get_client(
        cls, mode: GCPMode = GCPMode.MOCK, project_id: str = "pub-sub-kamo"
    ) -> GCPClientInterface:
        """지정된 모드(MOCK 또는 LIVE)와 프로젝트 ID에 해당하는 클라이언트를 반환합니다."""
        key = (mode, project_id)
        if key not in cls._instances:
            if mode == GCPMode.MOCK:
                cls._instances[key] = MockGCPClient(project_id=project_id)
            else:
                cls._instances[key] = LiveGCPClient(project_id=project_id)
        return cls._instances[key]

    @classmethod
    def reset(cls) -> None:
        """캐시된 싱글톤 인스턴스를 초기화합니다 (단위 테스트 격리용)."""
        cls._instances.clear()
