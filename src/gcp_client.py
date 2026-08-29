"""GCP client abstraction supporting both live Google Cloud and offline mock modes."""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import ClassVar


class GCPMode(str, Enum):
    """Execution mode for GCP operations."""

    LIVE = "live"
    MOCK = "mock"


@dataclass
class PublishedMessage:
    """Represents a message recorded in Pub/Sub."""

    message_id: str
    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)
    publish_time: float = 0.0


class GCPClientInterface(ABC):
    """Abstract interface for GCP Pub/Sub and Cloud Storage interactions."""

    def __init__(self, project_id: str, mode: GCPMode):
        self.project_id = project_id
        self.mode = mode

    @abstractmethod
    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
        """Publish message to a Pub/Sub topic and return message_id."""

    @abstractmethod
    def upload_blob(
        self,
        bucket_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload blob to GCS bucket and return gs:// URI."""

    @abstractmethod
    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        """Download blob content from GCS."""

    @abstractmethod
    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        """Delete blob from GCS."""


class MockGCPClient(GCPClientInterface):
    """In-memory mock client for local and offline testing."""

    def __init__(self, project_id: str):
        super().__init__(project_id=project_id, mode=GCPMode.MOCK)
        self.topics: dict[str, list[PublishedMessage]] = {}
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.subscribers: dict[str, list] = {}

    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
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
        """Retrieve all messages published to a topic (mock utility)."""
        return list(self.topics.get(topic_id, []))

    def clear_topic(self, topic_id: str) -> None:
        """Clear published messages in a topic."""
        self.topics[topic_id] = []

    def upload_blob(
        self,
        bucket_name: str,
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        if bucket_name not in self.buckets:
            self.buckets[bucket_name] = {}
        self.buckets[bucket_name][blob_name] = data
        return f"gs://{bucket_name}/{blob_name}"

    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        if bucket_name not in self.buckets or blob_name not in self.buckets[bucket_name]:
            raise KeyError(f"Blob gs://{bucket_name}/{blob_name} not found")
        return self.buckets[bucket_name][blob_name]

    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        if bucket_name in self.buckets and blob_name in self.buckets[bucket_name]:
            del self.buckets[bucket_name][blob_name]


class LiveGCPClient(GCPClientInterface):
    """Live GCP client connecting to real Pub/Sub and Storage services."""

    def __init__(self, project_id: str):
        super().__init__(project_id=project_id, mode=GCPMode.LIVE)
        self._publisher = None
        self._storage_client = None

    @property
    def publisher(self):
        if self._publisher is None:
            from google.cloud import pubsub_v1

            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    @property
    def storage(self):
        if self._storage_client is None:
            from google.cloud import storage

            self._storage_client = storage.Client(project=self.project_id)
        return self._storage_client

    def publish(
        self, topic_id: str, data: bytes, attributes: dict[str, str] | None = None
    ) -> str:
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
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{bucket_name}/{blob_name}"

    def download_blob(self, bucket_name: str, blob_name: str) -> bytes:
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()

    def delete_blob(self, bucket_name: str, blob_name: str) -> None:
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()


class GCPClientFactory:
    """Factory for creating GCP client instances."""

    _instances: ClassVar[dict[tuple[GCPMode, str], GCPClientInterface]] = {}

    @classmethod
    def get_client(
        cls, mode: GCPMode = GCPMode.MOCK, project_id: str = "pub-sub-kamo"
    ) -> GCPClientInterface:
        key = (mode, project_id)
        if key not in cls._instances:
            if mode == GCPMode.MOCK:
                cls._instances[key] = MockGCPClient(project_id=project_id)
            else:
                cls._instances[key] = LiveGCPClient(project_id=project_id)
        return cls._instances[key]

    @classmethod
    def reset(cls) -> None:
        """Clear singleton instances (useful in tests)."""
        cls._instances.clear()
