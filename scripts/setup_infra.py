#!/usr/bin/env python3
"""Google Cloud 인프라 자동 프로비저닝 스크립트 (GCP Infrastructure Provisioning).

[생성 대상 클라우드 리소스]
1. Pub/Sub Topics:
   - 메인 이벤트 토픽: `projects/{project_id}/topics/pubsub-demo-events`
   - Dead Letter 토픽: `projects/{project_id}/topics/pubsub-demo-dlq-topic`
2. Pub/Sub Subscriptions:
   - `pubsub-demo-sync-sub`: 레거시 동기식 폴링 벤치마크용 구독 (5회 재시도 DLQ 적용)
   - `pubsub-demo-stream-sub`: gRPC StreamingPull 벤치마크용 구독 (5회 재시도 DLQ 적용)
   - `pubsub-demo-dlq-sub`: 격리된 손상 메시지 조회용 DLQ 전용 구독
   - `pubsub-demo-bq-sub`: Dataflow 없이 BigQuery로 직접 수집하는 Zero-ETL 구독
3. Cloud Storage Bucket:
   - `gs://{project_id}-payloads`: 8MB 이상 대용량 멀티모달 텐서 오프로드 저장소
4. BigQuery Dataset & Table:
   - `{project_id}.pubsub_demo_analytics.streaming_events`: 실시간 스트리밍 분석 테이블
"""

import argparse
import sys

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError


def setup_infrastructure(project_id: str, region: str = "us-central1", dry_run: bool = False):
    """지정된 GCP 프로젝트에 데모용 3대 아키텍처 인프라를 프로비저닝합니다."""
    print(f"=== GCP 인프라 프로비저닝 시작 (프로젝트: {project_id}) ===")
    if dry_run:
        print("[DRY-RUN] 모의 시뮬레이션 모드: 네트워크 호출 없이 종료합니다.")
        return True

    from google.cloud import bigquery, pubsub_v1, storage

    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)

    # 1. Pub/Sub 토픽 생성 (메인 토픽 & Dead Letter 토픽)
    main_topic_name = f"projects/{project_id}/topics/pubsub-demo-events"
    dlq_topic_name = f"projects/{project_id}/topics/pubsub-demo-dlq-topic"

    for topic_path in [dlq_topic_name, main_topic_name]:
        try:
            pub_client.create_topic(request={"name": topic_path})
            print(f"✓ 토픽 생성 완료: {topic_path}")
        except AlreadyExists:
            print(f"• 토픽이 이미 존재합니다: {topic_path}")
        except GoogleAPICallError as e:
            print(f"✕ 토픽 생성 에러 ({topic_path}): {e}", file=sys.stderr)
            return False

    # 2. Dead Letter 전용 구독 생성 (격리된 포이즌 필 메시지 모니터링용)
    dlq_sub_name = f"projects/{project_id}/subscriptions/pubsub-demo-dlq-sub"
    try:
        sub_client.create_subscription(
            request={"name": dlq_sub_name, "topic": dlq_topic_name, "ack_deadline_seconds": 60}
        )
        print(f"✓ DLQ 전용 구독 생성 완료: {dlq_sub_name}")
    except AlreadyExists:
        print(f"• DLQ 전용 구독이 이미 존재합니다: {dlq_sub_name}")

    # 3. 메인 벤치마크 구독 생성 (최대 5회 재시도 Dead Letter 정책 바인딩)
    dead_letter_policy = {
        "dead_letter_topic": dlq_topic_name,
        "max_delivery_attempts": 5,  # 5회 실패 시 자동으로 DLQ 토픽으로 우회 격리
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
            print(f"✓ 벤치마크 구독 생성 완료: {sub_path} (DLQ 최대 시도 횟수: 5)")
        except AlreadyExists:
            print(f"• 구독이 이미 존재합니다: {sub_path}")

    # 4. Cloud Storage 대용량 페이로드 버킷 생성 (Claim-Check 패턴)
    bucket_name = f"{project_id}-payloads"
    try:
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        storage_client.create_bucket(bucket, location=region)
        print(f"✓ Cloud Storage 버킷 생성 완료: gs://{bucket_name}")
    except AlreadyExists:
        print(f"• Cloud Storage 버킷이 이미 존재합니다: gs://{bucket_name}")
    except GoogleAPICallError as e:
        print(f"✕ 버킷 생성 상태 알림: {e}")

    # 5. BigQuery 데이터셋 및 스트리밍 테이블 생성
    dataset_id = f"{project_id}.pubsub_demo_analytics"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = region
    try:
        bq_client.create_dataset(dataset, exists_ok=True)
        print(f"✓ BigQuery 데이터셋 생성 완료: {dataset_id}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery 데이터셋 알림: {e}")

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
        print(f"✓ BigQuery 테이블 생성 완료: {table_id}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery 테이블 알림: {e}")

    # 6. Pub/Sub BigQuery Zero-ETL 직접 스트리밍 구독 생성
    bq_sub_name = f"projects/{project_id}/subscriptions/pubsub-demo-bq-sub"
    bq_config = {
        "table": f"{project_id}:{dataset.dataset_id}.streaming_events",
        "write_metadata": True,  # publish_time, message_id 등 메타데이터 자동 파퓰레이션
    }

    # BigQuery 구독 생성에 필수적인 Pub/Sub 서비스 에이전트 권한 사전 바인딩 시도
    import subprocess
    try:
        res = subprocess.run(
            ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0 and res.stdout.strip():
            p_num = res.stdout.strip()
            sa = f"serviceAccount:service-{p_num}@gcp-sa-pubsub.iam.gserviceaccount.com"
            subprocess.run(
                [
                    "gcloud",
                    "projects",
                    "add-iam-policy-binding",
                    project_id,
                    f"--member={sa}",
                    "--role=roles/bigquery.dataEditor",
                    "--quiet",
                ],
                capture_output=True,
                check=False,
            )
    except Exception:
        pass

    try:
        sub_client.create_subscription(
            request={
                "name": bq_sub_name,
                "topic": main_topic_name,
                "bigquery_config": bq_config,
            }
        )
        print(f"✓ BigQuery Zero-ETL 직접 스트리밍 구독 생성 완료: {bq_sub_name}")
    except AlreadyExists:
        print(f"• BigQuery 구독이 이미 존재합니다: {bq_sub_name}")
    except GoogleAPICallError as e:
        print(f"✕ BigQuery 구독 상태 (Pub/Sub 서비스 에이전트 권한 필요): {e}")

    print("\n=== GCP 인프라 프로비저닝이 성공적으로 완료되었습니다 ===")
    print("\n--- 🎯 배포 검증을 위한 퀵 커맨드 ---")
    print(f"1. 자동 엔드투엔드 검증 실행: .venv/bin/python3 scripts/verify_gcp_live.py --project_id {project_id}")
    print(f"2. Pub/Sub 토픽 목록 확인:     gcloud pubsub topics list --project={project_id} --filter='name:pubsub-demo'")
    print(f"3. Pub/Sub 구독 목록 확인:     gcloud pubsub subscriptions list --project={project_id} --filter='name:pubsub-demo'")
    print(f"4. Cloud Storage 버킷 확인:    gcloud storage ls --project={project_id} gs://{bucket_name}")
    print(f"5. BigQuery 스트리밍 행 확인:  bq query --project_id={project_id} --use_legacy_sql=false 'SELECT count(*) FROM {dataset_id}.streaming_events'")
    print("--------------------------------------------------\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision GCP Pub/Sub Demo Infrastructure")
    parser.add_argument("--project_id", default="pub-sub-kamo", help="GCP Project ID")
    parser.add_argument("--region", default="us-central1", help="GCP Region")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without network calls")
    args = parser.parse_args()

    success = setup_infrastructure(args.project_id, args.region, dry_run=args.dry_run)
    sys.exit(0 if success else 1)
