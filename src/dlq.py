"""Dead Letter Queue (DLQ) manager and error injection flow."""

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from src.gcp_client import GCPClientInterface, PublishedMessage


class DLQManager:
    """Manages Dead Letter Queue lifecycle:

    - Tracks delivery attempts for received messages.
    - Allows retry attempts up to max_delivery_attempts (default 5).
    - Automatically routes poison pills / corrupted events to the DLQ topic upon exhaustion.
    """

    def __init__(
        self,
        client: GCPClientInterface,
        main_topic_id: str,
        dlq_topic_id: str,
        max_delivery_attempts: int = 5,
    ):
        self.client = client
        self.main_topic_id = main_topic_id
        self.dlq_topic_id = dlq_topic_id
        self.max_delivery_attempts = max_delivery_attempts
        self._delivery_attempts: dict[str, int] = defaultdict(int)

    def process_with_dlq(
        self,
        message: PublishedMessage,
        consumer_func: Callable[[bytes, dict[str, str]], Any],
    ) -> dict[str, Any]:
        """Execute message consumption within a DLQ-protected attempt boundary."""
        msg_id = message.message_id
        self._delivery_attempts[msg_id] += 1
        attempts = self._delivery_attempts[msg_id]

        try:
            result = consumer_func(message.data, message.attributes)
            return {
                "status": "success",
                "attempts": attempts,
                "result": result,
            }
        except Exception as exc:  # noqa: BLE001 - DLQ boundary catches all application faults
            error_msg = str(exc)
            if attempts < self.max_delivery_attempts:
                return {
                    "status": "retry",
                    "attempts": attempts,
                    "error": error_msg,
                }

            # Max attempts reached: isolate poison pill into Dead Letter Queue
            dlq_attributes = dict(message.attributes)
            dlq_attributes["quarantine-reason"] = error_msg
            dlq_attributes["delivery-attempts"] = str(attempts)

            self.client.publish(
                topic_id=self.dlq_topic_id,
                data=message.data,
                attributes=dlq_attributes,
            )

            return {
                "status": "dead_lettered",
                "attempts": attempts,
                "error": error_msg,
            }

    def get_quarantined_messages(self) -> list[PublishedMessage]:
        """Retrieve all currently quarantined messages from the DLQ topic (mock mode)."""
        if hasattr(self.client, "get_published_messages"):
            return self.client.get_published_messages(self.dlq_topic_id)
        return []
