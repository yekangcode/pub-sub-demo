#!/usr/bin/env python3
"""Google Cloud 데모 리소스 일괄 정리 스크립트 (GCP Infrastructure Teardown).

[삭제 대상 리소스]
1. Pub/Sub 구독 4개: `pubsub-demo-sync-sub`, `pubsub-demo-stream-sub`, `pubsub-demo-dlq-sub`, `pubsub-demo-bq-sub`
2. Pub/Sub 토픽 2개: `pubsub-demo-events`, `pubsub-demo-dlq-topic`
3. BigQuery 데이터셋 및 테이블: `pubsub_demo_analytics` (delete_contents=True)
4. Cloud Storage 버킷: `{project_id}-payloads` (force=True)

실수로 인한 운영 자원 삭제를 방지하기 위해 `--confirm` 플래그가 필수입니다.
"""

import argparse
import sys

from google.api_core.exceptions import GoogleAPICallError, NotFound


def cleanup_infrastructure(project_id: str, confirm: bool = False, dry_run: bool = False):
    """프로비저닝된 데모용 리소스를 안전하게 순차 삭제합니다."""
    if not confirm and not dry_run:
        print("오류: GCP 데모 리소스를 삭제하려면 반드시 --confirm 플래그를 지정해야 합니다.", file=sys.stderr)
        return False

    print(f"=== GCP 인프라 자원 회수 시작 (프로젝트: {project_id}) ===")
    if dry_run:
        print("[DRY-RUN] 모의 시뮬레이션 모드: 삭제 호출 없이 종료합니다.")
        return True

    from google.cloud import bigquery, pubsub_v1, storage

    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)

    # 1. Pub/Sub 구독 삭제
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
            print(f"✓ 구독 삭제 완료: {sub_path}")
        except NotFound:
            print(f"• 구독을 찾을 수 없습니다 (이미 삭제됨): {sub_path}")
        except GoogleAPICallError as e:
            print(f"✕ 구독 삭제 상태 알림 ({sub_path}): {e}")

    # 2. Pub/Sub 토픽 삭제
    topics = [
        "pubsub-demo-events",
        "pubsub-demo-dlq-topic",
    ]
    for topic in topics:
        topic_path = f"projects/{project_id}/topics/{topic}"
        try:
            pub_client.delete_topic(request={"topic": topic_path})
            print(f"✓ 토픽 삭제 완료: {topic_path}")
        except NotFound:
            print(f"• 토픽을 찾을 수 없습니다 (이미 삭제됨): {topic_path}")
        except GoogleAPICallError as e:
            print(f"✕ 토픽 삭제 상태 알림 ({topic_path}): {e}")

    # 3. BigQuery 데이터셋 및 테이블 일괄 삭제
    dataset_id = f"{project_id}.pubsub_demo_analytics"
    try:
        bq_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
        print(f"✓ BigQuery 데이터셋 및 테이블 삭제 완료: {dataset_id}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery 데이터셋 삭제 상태 알림: {e}")

    # 4. Cloud Storage 버킷 삭제
    bucket_name = f"{project_id}-payloads"
    try:
        bucket = storage_client.get_bucket(bucket_name)
        bucket.delete(force=True)
        print(f"✓ Cloud Storage 버킷 삭제 완료: gs://{bucket_name}")
    except NotFound:
        print(f"• Cloud Storage 버킷을 찾을 수 없습니다 (이미 삭제됨): gs://{bucket_name}")
    except GoogleAPICallError as e:
        print(f"✕ 버킷 삭제 상태 알림: {e}")

    print("\n=== GCP 인프라 자원 회수가 안전하게 완료되었습니다 ===")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Teardown GCP Pub/Sub Demo Infrastructure")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--confirm", action="store_true", help="Confirm deletion")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")
    args = parser.parse_args()

    success = cleanup_infrastructure(args.project_id, confirm=args.confirm, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
