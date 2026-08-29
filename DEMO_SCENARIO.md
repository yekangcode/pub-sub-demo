# ⚡ Google Cloud Pub/Sub: Anthropic 아키텍처 고객 데모 시나리오

본 문서는 고객사(소프트웨어 엔지니어링 리드, 인프라/플랫폼 아키텍트, 데이터 팀)를 대상으로 **Anthropic이 Google Cloud Pub/Sub을 기반으로 대규모 실시간 스트리밍 시스템을 구축한 3대 핵심 아키텍처**를 효과적으로 시연하기 위한 실전 데모 가이드입니다.

---

## 📋 데모 개요 및 아젠다

| 순서 | 데모 세션 | 핵심 시연 내용 | 소요 시간 |
| :--- | :--- | :--- | :---: |
| **0** | **환경 준비 & 시작** | 가상환경 활성화, Protobuf 컴파일, 대시보드 구동 | 2분 |
| **1** | **아키텍처 인트로** | Anthropic 아키텍처 3대 핵심 축 및 파이프라인 흐름 소개 | 3분 |
| **2** | **심층 분석 1: 이중 경로 수집 & Zstd 압축** | <8MB Fast Path vs >=8MB GCS 오프로드 및 투명한 복원 | 5분 |
| **3** | **심층 분석 2: 지연 시간 88% 절감 (StreamingPull)** | 동기식 폴링(~95ms) vs 양방향 gRPC 스트리밍(~11ms) 실측 비교 | 5분 |
| **4** | **심층 분석 3: Proto-First & DLQ 격리** | 포이즌 필 인위적 주입, 5회 재시도 차단 및 Dead Letter Queue 격리 | 5분 |
| **5** | **GCP 인프라 & BigQuery Zero-ETL (옵션)** | `pub-sub-kamo` 실환경 프로비저닝 및 무중단 파이프라인 정리 | 5분 |

---

## 🛠️ 세션 0: 환경 준비 및 대시보드 실행

### 1. 터미널 명령어
```bash
# 1. 프로젝트 디렉토리 이동
cd /usr/local/google/home/kangrenee/pub-sub-demo

# 2. 통합 원클릭 실행 스크립트 가동
./run_demo.sh
```

### 2. 기대되는 출력 결과
```text
============================================================
⚡ Starting Google Cloud Pub/Sub Anthropic Architecture Demo
============================================================
Compiling Protocol Buffers schema...
Schema compiled successfully: src/proto/streaming_event_pb2.py
Launching Streamlit Web Dashboard...
Open your browser at: http://localhost:8501

  You can now view your Streamlit app in your browser.

  URL: http://localhost:8501
```

### 3. 브라우저 접속
* 웹 브라우저에서 `http://localhost:8501` 접속
* 좌측 사이드바 `🌐 Language / 언어`에서 **한국어** 선택 (기본값)
* 모드는 기본값인 **모의 샌드박스 (로컬 / 오프라인)**로 진행

---

## 🏛️ 세션 1: 아키텍처 개요 (Intro & Tab 1)

### 1. 발표자 액션
* 상단의 `🏛️ 아키텍처 개요` 탭을 선택합니다.
* 화면에 표시되는 **Mermaid 파이프라인 다이어그램**을 화면 공유합니다.

### 2. 기대 화면
* Claude 서빙/에이전트 인스턴스 → `DualPathPublisher` → Zstd 압축 → 8MB 분기 → Fast Path(Pub/Sub) / Offload Path(GCS) → 영구 gRPC StreamingPull(컨슈머) & BigQuery Zero-ETL로 이어지는 전체 아키텍처 맵 렌더링.
* 상단 배너에 [How Anthropic Built on Google Cloud Pub/Sub (PDF 원본)](https://content-cdn.sessionboard.com/content/IdZQpQJIQVmsSBjedvUW_BRK1-041.pdf) 바로가기 링크 표시.

### 3. 핵심 스피킹 포인트 (Talking Points)
> *"대다수 기업들이 Kafka 클러스터를 직접 프로비저닝하면서 파티션 리밸런싱, 디스크 증설, 브로커 관리 부하에 시달립니다. 하지만 Anthropic은 완전 관리형인 **Google Cloud Pub/Sub**을 선택했습니다. 수천만 QPS의 트래픽 급증에도 파티션 재분배 없이 탄력적으로 스케일링되며, 오늘 시연할 세 가지 고도화 기법을 통해 대용량 멀티모달 처리와 극초저지연을 모두 달성했습니다."*

---

## 📦 세션 2: 이중 경로(Dual-Path) 수집 & Zstandard 압축 (Tab 2)

### 1. 배경 설명
* Pub/Sub 메시지의 단일 크기 제한은 **10MB**입니다.
* LLM 서빙 환경에서는 작은 텍스트 프롬프트부터 거대한 이미지, 고차원 임베딩 텐서(8MB+)까지 다양한 데이터가 들어옵니다.

### 2. 발표자 액션 & 시연
1. 사이드바에서 `배치 크기`를 `10`, `대용량 페이로드 비율`을 `20%`로 설정.
2. `🚀 합성 트래픽 배치 생성` 버튼 클릭.
3. 곧이어 `🚨 대용량 멀티모달 페이로드 1건 주입 (>8MB)` 버튼 클릭.

### 3. 기대되는 화면 결과
* **상단 메트릭 카드 4종**:
  - `패스트 패스 이벤트 (< 8MB)`: 8건
  - `GCS 오프로드 이벤트 (>= 8MB)`: 3건 (배치 중 2건 + 단독 주입 1건)
  - `절감된 네트워크 대역폭`: 약 **68.4% 절감** 표시 (Zstd 실시간 압축 효과)
  - `총 전송 페이로드`: 수십 MB 누적 집계
* **실시간 이벤트 로그 테이블**:
  - `path`: `FAST_PATH` vs `GCS_OFFLOAD`
  - `payload_uri`: `inline` vs `gs://pub-sub-kamo-payloads/payloads/evt-...bin`
  - `savings_%`: 각 이벤트별 60~78% 압축률 및 SHA-256 스키마 핑거프린트 확인

### 4. CLI 검증 명령어 (원격/터미널 시연 시)
```bash
.venv/bin/python3 -c "
from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher
from src.consumer import DualPathConsumer

client = GCPClientFactory.get_client(GCPMode.MOCK)
pub = DualPathPublisher(client, 'demo-topic', 'demo-bucket', offload_threshold_bytes=50000)
consumer = DualPathConsumer(client)

# 대용량 이벤트 발행 (100KB)
res = pub.publish_event('user-msg', b'X' * 100000)
print(f'경로: {res.path.value} | URI: {res.payload_uri} | 압축률: {res.reduction_percentage:.1f}%')

# 컨슈머 수신 (투명한 복원)
msg = client.get_published_messages('demo-topic')[0]
event = consumer.consume_message(msg)
print(f'컨슈머 복원 완료: Event ID={event.event_id}, Payload 크기={len(event.payload)} bytes')
"
```
**출력 결과**:
```text
경로: GCS_OFFLOAD | URI: gs://demo-bucket/payloads/evt-...bin | 압축률: 99.9%
컨슈머 복원 완료: Event ID=evt-..., Payload 크기=100000 bytes
```

### 5. 핵심 스피킹 포인트
> *"대형 바이너리가 들어와도 Pub/Sub 브로커 한도(10MB)를 절대 초과하지 않으며, 다운스트림 서비스는 GCS에 갔는지 인라인인지 신경 쓸 필요 없이 동일한 Protobuf 모델로 투명하게 객체를 복원합니다."*

---

## 🚀 세션 3: 지연 시간 88% 절감 — StreamingPull vs Sync Pull (Tab 3)

### 1. 배경 설명
* 기존 마이크로서비스는 대개 HTTP/gRPC를 통한 단방향 주기적 폴링(`Sync Pull`)을 사용합니다. 이는 유휴 대기 시간과 지속적인 연결 생성 비용을 발생시킵니다.
* Anthropic은 이를 **영구 양방향 gRPC 스트리밍(`StreamingPull`)**으로 전환하여 브로커가 메시지를 즉시 푸시하도록 개선했습니다.

### 2. 발표자 액션 & 시연
1. 좌측의 `동기식 Sync Pull 배치 실행 (10건)` 버튼 클릭.
2. 우측의 `gRPC StreamingPull 스트림 실행 (10건)` 버튼 클릭.

### 3. 기대되는 화면 결과
* **정보 배너**:
  - `Sync Pull로 10건 수신 완료. 평균 지연 시간: ~92ms`
  - `StreamingPull로 10건 수신 완료. 평균 지연 시간: ~11ms`
* **실시간 통계 메트릭**:
  - `Sync Pull P50 지연 시간`: **~92.0 ms**
  - `StreamingPull P50 지연 시간`: **~11.0 ms**
  - `측정된 지연 시간 절감률`: **-88.0%** (초록색 Inverse Delta 뱃지)
  - `StreamingPull P99 지연 시간`: **~17.5 ms** (vs Sync P99: ~140 ms)
* **막대 차트**: P50, P90, P95, P99 구간별로 약 8배~10배 차이나는 레이턴시 분포가 직관적으로 시각화됨.

### 4. CLI 벤치마크 실행 명령어
```bash
.venv/bin/python3 -c "
from src.gcp_client import GCPClientFactory, GCPMode
from src.workers.sync_worker import SyncPullWorker
from src.workers.streaming_worker import StreamingPullWorker
from src.generator import SyntheticWorkloadGenerator
from src.publisher import DualPathPublisher
from src.metrics import MetricsCollector
import time

client = GCPClientFactory.get_client(GCPMode.MOCK)
pub = DualPathPublisher(client, 'bench-topic', 'bench-bucket')
gen = SyntheticWorkloadGenerator(pub)
metrics = MetricsCollector()

# Sync Pull 테스트
gen.generate_batch(10)
sync_w = SyncPullWorker(client, 'p', 's1', 'bench-topic', simulated_poll_delay_ms=95.0)
for p in sync_w.pull_batch(10):
    metrics.record_latency('sync', p.latency_ms)

# StreamingPull 테스트
gen.generate_batch(10)
stream_w = StreamingPullWorker(client, 'p', 's2', 'bench-topic', callback=lambda m: metrics.record_latency('stream', m.latency_ms), simulated_stream_delay_ms=11.0)
stream_w.start()
time.sleep(0.1)
stream_w.stop()

comp = metrics.compare('sync', 'stream')
print(f'Sync P50: {metrics.get_stats(\"sync\")[\"p50\"]:.1f}ms')
print(f'Streaming P50: {metrics.get_stats(\"stream\")[\"p50\"]:.1f}ms')
print(f'지연 시간 절감률: {comp[\"reduction_percent\"]:.1f}%')
"
```
**출력 결과**:
```text
Sync P50: 95.0ms
Streaming P50: 11.0ms
지연 시간 절감률: 88.4%
```

### 5. 핵심 스피킹 포인트
> *"StreamingPull은 영구 수립된 HTTP/2 gRPC 스트림을 통해 메시지를 수신 대기 없이 바로 전달받습니다. Anthropic이 모델 서빙 레이턴시를 88% 줄일 수 있었던 핵심 비밀이 바로 이 구조입니다."*

---

## 🛡️ 세션 4: Proto-First 거버넌스 & Dead Letter Queue (DLQ) (Tab 4)

### 1. 배경 설명
* 수천 명의 엔지니어와 에이전트가 이벤트를 발행할 때, 단 하나의 잘못된 스키마 변경이나 손상된 바이너리(Poison Pill)가 전체 파이프라인을 멈추게 할 수 있습니다(Head-of-Line Blocking).
* Anthropic은 엄격한 Protocol Buffers 스키마와 **정확히 5회의 재시도 후 DLQ로 격리하는 서킷 브레이커**를 운영합니다.

### 2. 발표자 액션 & 시연
1. `🚨 포이즌 필 / 스키마 손상 이벤트 주입` 버튼 클릭.

### 3. 기대되는 화면 결과
* **에러 알림 배너**:
  - `이벤트 evt-corrupt-...가 5회 전송 실패 후 Dead Letter Queue로 안전하게 격리되었습니다!`
* **DLQ 격리 모니터링 테이블**:
  - `event_id`: `evt-corrupt-...`
  - `attempts`: `5` (최대 시도 한도 도달)
  - `status`: `DEAD_LETTER_FORWARDED`
  - `quarantine_reason`: `Schema validation fault: missing required fields or unparseable protobuf frame`
  - `timestamp`: 격리된 정확한 시각 기록

### 4. CLI 격리 시뮬레이션 명령어
```bash
.venv/bin/python3 -c "
from src.gcp_client import GCPClientFactory, GCPMode
from src.dlq import DLQManager
from src.consumer import DualPathConsumer
from src.gcp_client import PubSubMessage

client = GCPClientFactory.get_client(GCPMode.MOCK)
dlq = DLQManager(client, 'main-topic', 'dlq-topic', max_delivery_attempts=5)
consumer = DualPathConsumer(client)

# 고의로 손상된 바이너리 메시지 생성
bad_msg = PubSubMessage(message_id='bad-001', data=b'MALFORMED_GARBAGE', attributes={'content-encoding': 'zstd'})

for attempt in range(1, 6):
    res = dlq.process_with_dlq(bad_msg, consumer.consume_message)
    print(f'시도 #{res[\"attempts\"]}: 상태={res[\"status\"]}')
"
```
**출력 결과**:
```text
시도 #1: 상태=RETRY
시도 #2: 상태=RETRY
시도 #3: 상태=RETRY
시도 #4: 상태=RETRY
시도 #5: 상태=DEAD_LETTER_FORWARDED
```

### 5. 핵심 스피킹 포인트
> *"손상된 페이로드가 정상 컨슈머를 무한 재시도로 마비시키지 않도록, 정확히 5회 실패 후 DLQ 토픽(`pubsub-demo-dlq-topic`)으로 즉시 격리하여 메인 파이프라인의 가용성(SLO 99.99%)을 완벽히 보장합니다."*

---

## ☁️ 세션 5: 실제 Google Cloud 프로젝트 라이브 검증 (pub-sub-kamo)

실제 Google Cloud 프로젝트(`pub-sub-kamo`)에서 5대 아키텍처 항목을 직접 배포하고 검증할 때 사용하는 실전 가이드입니다.

### 1. 사전 권한 점검 (Pre-requisites & IAM)
Pub/Sub이 **Dead Letter Topic으로 메시지를 우회 격리**하고 **BigQuery로 Zero-ETL 스트리밍 쓰기**를 수행하려면 Google 관리형 Pub/Sub 서비스 에이전트의 IAM 권한이 필수입니다.

```bash
PROJECT_ID="pub-sub-kamo"
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

# 1. Dead Letter Topic 발행 권한
gcloud pubsub topics add-iam-policy-binding pubsub-demo-dlq-topic \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/pubsub.publisher"

# 2. 메인 구독 메시지 확인(Ack) 권한
gcloud pubsub subscriptions add-iam-policy-binding pubsub-demo-stream-sub \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/pubsub.subscriber"

# 3. BigQuery 테이블 쓰기 권한 (Zero-ETL)
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/bigquery.dataEditor"
```

---

### 2. 인프라 프로비저닝 (Setup Infrastructure)
```bash
# GCP 인프라 일괄 프로비저닝 (Topics, Subscriptions, GCS Bucket, BigQuery Dataset & Table)
.venv/bin/python3 scripts/setup_infra.py --project_id pub-sub-kamo
```
**기대 출력**:
```text
✓ Created Topic: projects/pub-sub-kamo/topics/pubsub-demo-events
✓ Created Topic: projects/pub-sub-kamo/topics/pubsub-demo-dlq-topic
✓ Created DLQ Subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-dlq-sub
✓ Created Benchmark Subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-sync-sub (DLQ max_attempts=5)
✓ Created Benchmark Subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-stream-sub (DLQ max_attempts=5)
✓ Created Storage Bucket: gs://pub-sub-kamo-payloads
✓ Created BigQuery Dataset: pub-sub-kamo.pubsub_demo_analytics
✓ Created BigQuery Table: pub-sub-kamo.pubsub_demo_analytics.streaming_events
✓ Created BigQuery Zero-ETL Subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-bq-sub
=== Infrastructure Provisioning Completed Successfully ===
```

---

### 3. 실환경 5대 핵심 항목 1-Click 자동 검증
CLI 또는 Streamlit 대시보드 5번째 탭(`🔍 4. 실제 GCP 프로젝트 라이브 검증`)에서 한 번의 명령어로 전체 아키텍처를 실시간 검증합니다.

```bash
# 실서버 자동 검증 실행
.venv/bin/python3 scripts/verify_gcp_live.py --project_id pub-sub-kamo
```
**기대 출력**:
```text
Starting End-to-End Verification on Project: pub-sub-kamo (Mode: LIVE)
======================================================================
[1/5] 🔍 Pre-flight & Pub/Sub Service Agent IAM Permissions
======================================================================
✓ Target Project ID: pub-sub-kamo
✓ Pub/Sub Service Agent: service-...@gcp-sa-pubsub.iam.gserviceaccount.com
✓ Required IAM bindings verified.

======================================================================
[2/5] 🔍 Dual-Path Ingestion Pattern & GCS Offload Verification
======================================================================
• Case 2A (Fast Path): Event ID=evt-small-001, Path=fast
  ✓ Inline Pub/Sub message successfully verified.
• Case 2B (GCS Offload): Event ID=evt-large-001, Path=gcs_offload
  GCS URI: gs://pub-sub-kamo-payloads/payloads/evt-large-001.bin
  ✓ Consumer transparently fetched from GCS and reconstituted payload! (SHA-256 match)

======================================================================
[3/5] 🔍 StreamingPull vs Synchronous Pull Latency Benchmark (88% Reduction)
======================================================================
• Sync Pull (Batch Polling) P50 Latency: 98.4 ms
• StreamingPull (Persistent gRPC) P50 Latency: 12.1 ms
✓ Measured Latency Drop: 87.7% (Anthropic target: ~88% reduction achieved)

======================================================================
[4/5] 🔍 Dead Letter Queue (DLQ) 5-Retry Quarantine Verification
======================================================================
  Attempt #1: status=retry
  Attempt #2: status=retry
  Attempt #3: status=retry
  Attempt #4: status=retry
  Attempt #5: status=dead_lettered
✓ Poison pill circuit-breaker triggered after 5 attempts -> Forwarded to DLQ: pubsub-demo-dlq-topic

======================================================================
[5/5] 🔍 BigQuery Zero-ETL Subscription & Analytics Verification
======================================================================
• BigQuery Target: pub-sub-kamo.pubsub_demo_analytics.streaming_events
• Subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-bq-sub
• Ingestion Mode: Direct Pub/Sub to BigQuery Storage Write API (Zero-ETL, No Dataflow)
✓ BigQuery Streaming Row Count: 10
======================================================================
🎉 ALL 5 LIVE GCP ARCHITECTURE VERIFICATION CHECKS PASSED!
======================================================================
```

---

### 4. BigQuery 및 GCS 개별 리소스 CLI 확인

#### ① BigQuery 실시간 적재 데이터 확인 (Zero-ETL)
```bash
bq query --use_legacy_sql=false '
SELECT
  subscription_name,
  message_id,
  publish_time,
  attributes
FROM
  `pub-sub-kamo.pubsub_demo_analytics.streaming_events`
ORDER BY
  publish_time DESC
LIMIT 5;
'
```

#### ② GCS 오프로드 페이로드 객체 확인
```bash
gcloud storage ls -l gs://pub-sub-kamo-payloads/payloads/
```

#### ③ DLQ 격리 토픽에 전송된 손상 메시지 풀링 확인
```bash
gcloud pubsub subscriptions pull pubsub-demo-dlq-sub --auto-ack --limit=5
```

---

### 5. 데모 종료 후 안전한 자원 회수 (Cleanup)
```bash
# 유휴 비용 방지를 위한 리소스 일괄 삭제
.venv/bin/python3 scripts/cleanup_infra.py --project_id pub-sub-kamo --confirm
```
**기대 출력**:
```text
Deleted subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-bq-sub
Deleted subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-stream-sub
Deleted subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-sync-sub
Deleted subscription: projects/pub-sub-kamo/subscriptions/pubsub-demo-dlq-sub
Deleted topic: projects/pub-sub-kamo/topics/pubsub-demo-events
Deleted topic: projects/pub-sub-kamo/topics/pubsub-demo-dlq-topic
Purged GCS bucket: gs://pub-sub-kamo-payloads
Deleted BigQuery dataset: pub-sub-kamo.pubsub_demo_analytics
Infrastructure cleanup complete!
```

---

## 🎯 질의응답(Q&A) 대비 치트시트

1. **Q: Kafka 대비 Pub/Sub의 가장 큰 장점은 무엇인가요?**
   - **A**: 무관리(No-Ops)와 무제한 파티션 확장성입니다. Kafka는 트래픽 스파이크 시 파티션 리밸런싱 지연과 브로커 디스크 OOM이 발생하지만, Pub/Sub은 샤딩 관리가 필요 없이 100% 자동 확장됩니다.
2. **Q: 8MB 이상의 대용량 데이터는 실시간성이 떨어지지 않나요?**
   - **A**: Cloud Storage의 고성능 멀티파트 업로드와 결합되며, 대부분의 실시간 이벤트는 Fast Path(<8MB)로 처리됩니다. 초대형 데이터는 메타데이터 포인터만 Pub/Sub을 거치므로 브로커 전송 지연과 비용을 동시에 최적화합니다.
3. **Q: BigQuery 구독은 Dataflow와 어떻게 다른가요?**
   - **A**: 별도의 Apache Beam/Dataflow 파이프라인이나 워커 VM을 띄울 필요 없이, Pub/Sub 브로커가 직접 BigQuery Storage Write API로 스트리밍 인서트합니다(Zero-ETL). 운영 복잡도와 인프라 비용이 대폭 절감됩니다.
