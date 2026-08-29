import time

from src.gcp_client import GCPClientFactory, GCPMode
from src.workers.streaming_worker import StreamingPullWorker


def test_streaming_pull_worker_callback_execution():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-streaming-topic"
    sub_id = "test-streaming-sub"

    client.publish(topic_id=topic_id, data=b"stream-msg-1", attributes={"type": "stream"})
    client.publish(topic_id=topic_id, data=b"stream-msg-2", attributes={"type": "stream"})

    received_messages = []

    def callback(msg):
        received_messages.append(msg)

    worker = StreamingPullWorker(
        client=client,
        project_id="pub-sub-kamo",
        subscription_id=sub_id,
        topic_id=topic_id,
        callback=callback,
        simulated_stream_delay_ms=5.0,  # ~5ms gRPC streaming latency
    )

    worker.start()
    time.sleep(0.05)
    worker.stop()

    assert len(received_messages) == 2
    assert received_messages[0].data == b"stream-msg-1"
    assert received_messages[1].data == b"stream-msg-2"
    assert received_messages[0].latency_ms < 20.0  # Much faster than sync pull
