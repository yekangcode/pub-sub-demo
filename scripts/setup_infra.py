#!/usr/bin/env python3
"""GCP Infrastructure Provisioning Script for Pub/Sub Demo (pub-sub-kamo).

Creates:
1. Pub/Sub Topics: Main (`pubsub-demo-events`) & DLQ (`pubsub-demo-dlq-topic`)
2. Pub/Sub Subscriptions:
   - `pubsub-demo-sync-sub` (Sync pull, 5-retry DLQ)
   - `pubsub-demo-stream-sub` (StreamingPull, 5-retry DLQ)
   - `pubsub-demo-dlq-sub` (DLQ inspection)
   - `pubsub-demo-bq-sub` (BigQuery Zero-ETL subscription)
3. Cloud Storage Bucket: `{project_id}-payloads`
4. BigQuery Dataset & Table: `{project_id}.pubsub_demo_analytics.streaming_events`
"""

import argparse
import sys

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError


def setup_infrastructure(project_id: str, region: str = "us-central1", dry_run: bool = False):
    print(f"=== Starting GCP Infrastructure Setup for Project: {project_id} ===")
    if dry_run:
        print("[DRY-RUN] Simulating resource creation...")
        return True

    from google.cloud import bigquery, pubsub_v1, storage

    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)

    # 1. Topics
    main_topic_name = f"projects/{project_id}/topics/pubsub-demo-events"
    dlq_topic_name = f"projects/{project_id}/topics/pubsub-demo-dlq-topic"

    for topic_path in [dlq_topic_name, main_topic_name]:
        try:
            pub_client.create_topic(request={"name": topic_path})
            print(f"✓ Created Topic: {topic_path}")
        except AlreadyExists:
            print(f"• Topic already exists: {topic_path}")
        except GoogleAPICallError as e:
            print(f"✕ Error creating topic {topic_path}: {e}", file=sys.stderr)
            return False

    # 2. DLQ Subscription
    dlq_sub_name = f"projects/{project_id}/subscriptions/pubsub-demo-dlq-sub"
    try:
        sub_client.create_subscription(
            request={"name": dlq_sub_name, "topic": dlq_topic_name, "ack_deadline_seconds": 60}
        )
        print(f"✓ Created DLQ Subscription: {dlq_sub_name}")
    except AlreadyExists:
        print(f"• DLQ Subscription already exists: {dlq_sub_name}")

    # 3. Main Subscriptions with Dead Letter Policy
    dead_letter_policy = {
        "dead_letter_topic": dlq_topic_name,
        "max_delivery_attempts": 5,
    }

    sub_configs = [
        ("pubsub-demo-sync-sub", 20),
        ("pubsub-demo-stream-sub", 20),
    ]

    for sub_id, ack_deadline in sub_configs:
        sub_path = f"projects/{project_id}/subscriptions/{sub_id}"
        try:
            sub_client.create_subscription(
                request={
                    "name": sub_path,
                    "topic": main_topic_name,
                    "ack_deadline_seconds": ack_deadline,
                    "dead_letter_policy": dead_letter_policy,
                }
            )
            print(f"✓ Created Benchmark Subscription: {sub_path} (DLQ max_attempts=5)")
        except AlreadyExists:
            print(f"• Subscription already exists: {sub_path}")

    # 4. Cloud Storage Bucket
    bucket_name = f"{project_id}-payloads"
    try:
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        storage_client.create_bucket(bucket, location=region)
        print(f"✓ Created Storage Bucket: gs://{bucket_name}")
    except AlreadyExists:
        print(f"• Storage Bucket already exists: gs://{bucket_name}")
    except GoogleAPICallError as e:
        print(f"✕ Storage bucket notice: {e}")

    # 5. BigQuery Dataset & Table
    dataset_id = f"{project_id}.pubsub_demo_analytics"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = region
    try:
        bq_client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Created BigQuery Dataset: {dataset_id}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery dataset error: {e}")

    table_id = f"{dataset_id}.streaming_events"
    schema = [
        bigquery.SchemaField("subscription_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("publish_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data", "BYTES", mode="NULLABLE"),
        bigquery.SchemaField("attributes", "JSON", mode="NULLABLE"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    try:
        bq_client.create_table(table, exists_ok=True)
        print(f"✓ Created BigQuery Table: {table_id}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery table error: {e}")

    # 6. Pub/Sub BigQuery Subscription (Zero-ETL)
    bq_sub_name = f"projects/{project_id}/subscriptions/pubsub-demo-bq-sub"
    bq_config = {
        "table": f"{project_id}:{dataset.dataset_id}.streaming_events",
        "write_metadata": True,
    }
    try:
        sub_client.create_subscription(
            request={
                "name": bq_sub_name,
                "topic": main_topic_name,
                "bigquery_config": bq_config,
            }
        )
        print(f"✓ Created BigQuery Zero-ETL Subscription: {bq_sub_name}")
    except AlreadyExists:
        print(f"• BigQuery Subscription already exists: {bq_sub_name}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery subscription notice (requires Pub/Sub SA permissions): {e}")

    print("\n=== Infrastructure Provisioning Completed Successfully ===")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision GCP Pub/Sub Demo Infrastructure")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")
    args = parser.parse_args()

    success = setup_infrastructure(args.project_id, args.region, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
