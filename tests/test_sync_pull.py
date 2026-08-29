from src.gcp_client import GCPClientFactory, GCPMode
from src.workers.sync_pull import SyncPullWorker


def test_sync_pull_worker_fetches_messages():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-sync-topic"
    sub_id = "test-sync-sub"

    client.publish(topic_id=topic_id, data=b"message-1", attributes={"type": "sync"})
    client.publish(topic_id=topic_id, data=b"message-2", attributes={"type": "sync"})

    worker = SyncPullWorker(
        client=client,
        project_id="pub-sub-kamo",
        subscription_id=sub_id,
        topic_id=topic_id,
        batch_size=5,
    )

    results = worker.pull_batch(max_messages=5)
    assert len(results) == 2
    assert results[0].data == b"message-1"
    assert results[1].data == b"message-2"
    assert results[0].latency_ms > 0.0


def test_sync_pull_worker_empty_topic():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    topic_id = "test-empty-sync-topic"
    sub_id = "test-empty-sync-sub"

    worker = SyncPullWorker(
        client=client,
        project_id="pub-sub-kamo",
        subscription_id=sub_id,
        topic_id=topic_id,
    )

    results = worker.pull_batch(max_messages=5)
    assert len(results) == 0
