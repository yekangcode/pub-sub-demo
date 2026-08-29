#!/usr/bin/env python3
"""GCP Infrastructure Teardown Script for Pub/Sub Demo (pub-sub-kamo).

Safely removes:
1. Subscriptions (`pubsub-demo-sync-sub`, `pubsub-demo-stream-sub`, `pubsub-demo-dlq-sub`, `pubsub-demo-bq-sub`)
2. Topics (`pubsub-demo-events`, `pubsub-demo-dlq-topic`)
3. BigQuery Dataset & Table (`pubsub_demo_analytics`)
4. GCS Bucket (`{project_id}-payloads`)
"""

import argparse
import sys

from google.api_core.exceptions import GoogleAPICallError, NotFound


def cleanup_infrastructure(project_id: str, confirm: bool = False, dry_run: bool = False):
    if not confirm and not dry_run:
        print("Error: Must specify --confirm to delete GCP demo resources.", file=sys.stderr)
        return False

    print(f"=== Starting GCP Infrastructure Teardown for Project: {project_id} ===")
    if dry_run:
        print("[DRY-RUN] Simulating resource deletion...")
        return True

    from google.cloud import bigquery, pubsub_v1, storage

    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)

    # 1. Delete Subscriptions
    subs = [
        "pubsub-demo-sync-sub",
        "pubsub-demo-stream-sub",
        "pubsub-demo-dlq-sub",
        "pubsub-demo-bq-sub",
    ]
    for sub in subs:
        sub_path = f"projects/{project_id}/subscriptions/{sub}"
        try:
            sub_client.delete_subscription(request={"subscription": sub_path})
            print(f"✓ Deleted Subscription: {sub_path}")
        except NotFound:
            print(f"• Subscription not found (already deleted): {sub_path}")
        except GoogleAPICallError as e:
            print(f"✕ Notice deleting subscription {sub_path}: {e}")

    # 2. Delete Topics
    topics = [
        "pubsub-demo-events",
        "pubsub-demo-dlq-topic",
    ]
    for topic in topics:
        topic_path = f"projects/{project_id}/topics/{topic}"
        try:
            pub_client.delete_topic(request={"topic": topic_path})
            print(f"✓ Deleted Topic: {topic_path}")
        except NotFound:
            print(f"• Topic not found (already deleted): {topic_path}")
        except GoogleAPICallError as e:
            print(f"✕ Notice deleting topic {topic_path}: {e}")

    # 3. Delete BigQuery Dataset & Tables
    dataset_id = f"{project_id}.pubsub_demo_analytics"
    try:
        bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"✓ Deleted BigQuery Dataset and Tables: {dataset_id}")
    except GoogleAPICallError as e:
        print(f"✕ Notice deleting BigQuery dataset: {e}")

    # 4. Delete GCS Bucket
    bucket_name = f"{project_id}-payloads"
    try:
        bucket = storage_client.get_bucket(bucket_name)
        bucket.delete(force=True)
        print(f"✓ Deleted GCS Bucket: gs://{bucket_name}")
    except NotFound:
        print(f"• GCS Bucket not found (already deleted): gs://{bucket_name}")
    except GoogleAPICallError as e:
        print(f"✕ Notice deleting GCS bucket: {e}")

    print("\n=== Infrastructure Teardown Completed Successfully ===")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teardown GCP Pub/Sub Demo Infrastructure")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--confirm", action="store_true", help="Confirm deletion")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")
    args = parser.parse_args()

    success = cleanup_infrastructure(args.project_id, confirm=args.confirm, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
