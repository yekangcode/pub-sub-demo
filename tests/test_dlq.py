from src.consumer import DualPathConsumer
from src.dlq import DLQManager
from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher


def test_dlq_retries_and_quarantine_after_5_attempts():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    main_topic = "dlq-main-topic"
    dlq_topic = "dlq-dead-letter-topic"

    publisher = DualPathPublisher(
        client=client,
        topic_id=main_topic,
        bucket_name="dlq-bucket",
    )
    consumer = DualPathConsumer(client=client)
    dlq_mgr = DLQManager(
        client=client,
        main_topic_id=main_topic,
        dlq_topic_id=dlq_topic,
        max_delivery_attempts=5,
    )

    # Publish an intentionally corrupted poison pill
    publisher.publish_event(
        event_id="poison-pill-01",
        source="stress-tester",
        payload=b"broken payload data",
        is_corrupted=True,
    )

    messages = client.get_published_messages(main_topic)
    assert len(messages) == 1
    raw_msg = messages[0]

    # Attempt 1 to 4: should fail and retry without routing to DLQ yet
    for attempt in range(1, 5):
        status = dlq_mgr.process_with_dlq(raw_msg, consumer.consume_message)
        assert status["status"] == "retry"
        assert status["attempts"] == attempt

    assert len(client.get_published_messages(dlq_topic)) == 0

    # 5th attempt: should exceed max_delivery_attempts and move to DLQ
    final_status = dlq_mgr.process_with_dlq(raw_msg, consumer.consume_message)
    assert final_status["status"] == "dead_lettered"
    assert final_status["attempts"] == 5

    # Verify dead lettered message is now quarantined in DLQ topic
    dlq_messages = client.get_published_messages(dlq_topic)
    assert len(dlq_messages) == 1
    assert dlq_messages[0].attributes["quarantine-reason"] == "Corrupted event detected: poison-pill-01"
    assert dlq_messages[0].attributes["delivery-attempts"] == "5"


def test_dlq_successful_message_does_not_retry():
    client = GCPClientFactory.get_client(mode=GCPMode.MOCK, project_id="pub-sub-kamo")
    client.clear_topic("dlq-success-main")
    client.clear_topic("dlq-success-dlq")

    publisher = DualPathPublisher(
        client=client,
        topic_id="dlq-success-main",
        bucket_name="dlq-bucket",
    )
    consumer = DualPathConsumer(client=client)
    dlq_mgr = DLQManager(
        client=client,
        main_topic_id="dlq-success-main",
        dlq_topic_id="dlq-success-dlq",
    )

    publisher.publish_event(
        event_id="valid-evt-01",
        source="good-actor",
        payload=b"valid payload data",
        is_corrupted=False,
    )

    raw_msg = client.get_published_messages("dlq-success-main")[0]
    status = dlq_mgr.process_with_dlq(raw_msg, consumer.consume_message)

    assert status["status"] == "success"
    assert status["attempts"] == 1
    assert len(client.get_published_messages("dlq-success-dlq")) == 0
