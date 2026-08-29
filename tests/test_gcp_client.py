import pytest

from src.gcp_client import GCPClientFactory, GCPMode


def test_mock_gcp_client_publish_and_retrieve():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")

    topic_id = "test-topic"
    msg_id = client.publish(topic_id=topic_id, data=b"hello-mock", attributes={"tag": "demo"})

    assert msg_id.startswith("mock-msg-")
    messages = client.get_published_messages(topic_id)
    assert len(messages) == 1
    assert messages[0].data == b"hello-mock"
    assert messages[0].attributes["tag"] == "demo"


def test_mock_gcp_client_storage_operations():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    bucket = "pubsub-kamo-test-bucket"
    blob_name = "test-blob.bin"
    payload = b"large-binary-blob-data"

    uri = client.upload_blob(bucket, blob_name, payload, content_type="application/octet-stream")
    assert uri == f"gs://{bucket}/{blob_name}"

    retrieved = client.download_blob(bucket, blob_name)
    assert retrieved == payload

    client.delete_blob(bucket, blob_name)
    with pytest.raises(KeyError):
        client.download_blob(bucket, blob_name)


def test_live_mode_factory_instantiation():
    client = GCPClientFactory.get_client(mode=GCPMode.LIVE, project_id="pub-sub-kamo")
    assert client.project_id == "pub-sub-kamo"
    assert client.mode == GCPMode.LIVE
