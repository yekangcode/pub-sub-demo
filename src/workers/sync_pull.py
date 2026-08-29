"""Synchronous Pull Worker implementation for Pub/Sub."""

import time
from dataclasses import dataclass, field

from src.gcp_client import GCPClientInterface, GCPMode


@dataclass
class PullMessageResult:
    """Result of a single pulled message."""

    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)
    message_id: str = ""
    ack_id: str = ""
    latency_ms: float = 0.0


class SyncPullWorker:
    """Worker performing synchronous batch pulling (Pull RPC) over HTTP/gRPC.

    Exhibits higher latency due to polling intervals and per-batch connection handshakes.
    """

    def __init__(
        self,
        client: GCPClientInterface,
        project_id: str,
        subscription_id: str,
        topic_id: str,
        batch_size: int = 10,
        simulated_poll_delay_ms: float = 45.0,  # Represents HTTP/batch round-trip penalty
    ):
        self.client = client
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.topic_id = topic_id
        self.batch_size = batch_size
        self.simulated_poll_delay_ms = simulated_poll_delay_ms
        self._subscriber = None

    @property
    def subscriber(self):
        if self._subscriber is None and self.client.mode == GCPMode.LIVE:
            from google.cloud import pubsub_v1

            self._subscriber = pubsub_v1.SubscriberClient()
        return self._subscriber

    def pull_batch(self, max_messages: int | None = None) -> list[PullMessageResult]:
        """Execute a synchronous pull request."""
        batch_limit = max_messages or self.batch_size
        start_time = time.perf_counter()
        results: list[PullMessageResult] = []

        if self.client.mode == GCPMode.MOCK:
            # Mock pulling: retrieve from topic
            messages = self.client.get_published_messages(self.topic_id)
            if not messages:
                return []

            # Simulate network round-trip overhead of synchronous pull
            if self.simulated_poll_delay_ms > 0:
                time.sleep(self.simulated_poll_delay_ms / 1000.0)

            to_process = messages[:batch_limit]
            # remove processed from topic
            self.client.topics[self.topic_id] = messages[batch_limit:]

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            per_msg_latency = elapsed_ms

            for msg in to_process:
                results.append(
                    PullMessageResult(
                        data=msg.data,
                        attributes=msg.attributes,
                        message_id=msg.message_id,
                        ack_id=f"ack-{msg.message_id}",
                        latency_ms=per_msg_latency,
                    )
                )
        else:
            # Live GCP Pub/Sub pull
            sub_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
            response = self.subscriber.pull(
                request={"subscription": sub_path, "max_messages": batch_limit}
            )

            ack_ids = []
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            per_msg_latency = elapsed_ms / max(1, len(response.received_messages))

            for received in response.received_messages:
                results.append(
                    PullMessageResult(
                        data=received.message.data,
                        attributes=dict(received.message.attributes),
                        message_id=received.message.message_id,
                        ack_id=received.ack_id,
                        latency_ms=per_msg_latency,
                    )
                )
                ack_ids.append(received.ack_id)

            if ack_ids:
                self.subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})

        return results
