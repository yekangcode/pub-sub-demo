"""Asynchronous gRPC StreamingPull Worker implementation for Pub/Sub."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.gcp_client import GCPClientInterface, GCPMode


@dataclass
class StreamingMessageResult:
    """Represents a message received over gRPC StreamingPull."""

    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)
    message_id: str = ""
    ack_id: str = ""
    latency_ms: float = 0.0


class StreamingPullWorker:
    """Asynchronous StreamingPull subscriber utilizing persistent bidirectional gRPC.

    Achieves sub-10ms delivery by keeping the stream open and avoiding polling round-trips.
    """

    def __init__(
        self,
        client: GCPClientInterface,
        project_id: str,
        subscription_id: str,
        topic_id: str,
        callback: Callable[[StreamingMessageResult], Any],
        simulated_stream_delay_ms: float = 6.0,  # Realistic gRPC streaming push delay
    ):
        self.client = client
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.topic_id = topic_id
        self.callback = callback
        self.simulated_stream_delay_ms = simulated_stream_delay_ms

        self._running = False
        self._thread: threading.Thread | None = None
        self._subscriber = None
        self._future = None

    @property
    def subscriber(self):
        if self._subscriber is None and self.client.mode == GCPMode.LIVE:
            from google.cloud import pubsub_v1

            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    def start(self) -> None:
        """Start receiving messages asynchronously via StreamingPull."""
        if self._running:
            return

        self._running = True

        if self.client.mode == GCPMode.MOCK:
            self._thread = threading.Thread(target=self._mock_stream_loop, daemon=True)
            self._thread.start()
        else:
            sub_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

            def _live_callback(message):
                start_ms = time.time() * 1000.0
                publish_time_ms = message.publish_time.timestamp() * 1000.0
                latency = max(1.0, start_ms - publish_time_ms)

                res = StreamingMessageResult(
                    data=message.data,
                    attributes=dict(message.attributes),
                    message_id=message.message_id,
                    ack_id=message.ack_id,
                    latency_ms=latency,
                )
                self.callback(res)
                message.ack()

            self._future = self.subscriber.subscribe(sub_path, callback=_live_callback)

    def _mock_stream_loop(self) -> None:
        """Background thread simulating persistent gRPC stream delivery."""
        while self._running:
            messages = self.client.get_published_messages(self.topic_id)
            if messages:
                # Take messages from queue
                to_process = list(messages)
                self.client.topics[self.topic_id] = []

                for msg in to_process:
                    if not self._running:
                        break
                    # Simulate low-latency streaming delivery
                    if self.simulated_stream_delay_ms > 0:
                        time.sleep(self.simulated_stream_delay_ms / 1000.0)

                    res = StreamingMessageResult(
                        data=msg.data,
                        attributes=msg.attributes,
                        message_id=msg.message_id,
                        ack_id=f"ack-{msg.message_id}",
                        latency_ms=self.simulated_stream_delay_ms,
                    )
                    self.callback(res)

            time.sleep(0.01)

    def stop(self) -> None:
        """Stop the streaming consumer and cleanly disconnect the stream."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
            self._thread = None

        if self._future:
            self._future.cancel()
            self._future = None
