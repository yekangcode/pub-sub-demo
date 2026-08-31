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
| **4** | **심층 분석 3: 포맷 최적화 & 1KB 과금 우회 & DLQ** | 바이너리 스키마(gRPC vs REST), BatchSettings(10배 과금 방지), 5회 DLQ 격리 | 6분 |
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
Schema compiled successfully: src/proto_gen/streaming_event_pb2.py
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
* **네트워크 대역폭 절감 2대 축 안내 배너**:
  - `1. Dual-Path 브로커 트래픽 절감 (98%+)`: 8MB 이상의 대용량 텐서/이미지는 Pub/Sub 브로커를 우회하여 GCS에 저장되며, 브로커에는 150B 포인터만 전송되어 브로커 과금 및 네트워크 병목을 99% 이상 제거.
  - `2. Zstandard 페이로드 압축 절감 (45~80%)`: 텍스트 프롬프트는 ~80%, 멀티모달 임베딩 텐서는 ~50%의 무손실 바이트 압축 달성.
* **상단 메트릭 카드 4종**:
  - `패스트 패스 이벤트 (< 8MB)`: 8건
  - `GCS 오프로드 이벤트 (>= 8MB)`: 3건 (배치 중 2건 + 단독 주입 1건)
  - `🎯 Pub/Sub 브로커 트래픽 절감`: 약 **98.5% 절감** 표시 (GCS Claim-Check 오프로드 효과)
  - `📦 Zstd 페이로드 압축 절감`: 약 **50.2% 절감** 표시 (Zstandard 실시간 압축 효과)
* **실시간 이벤트 로그 테이블**:
  - `path`: `fast` vs `gcs_offload`
  - `payload_uri`: `inline` vs `gs://pub-sub-kamo-payloads/payloads/evt-...bin`
  - `pubsub_wire_bytes`: 8MB 대신 **~150B~250B**만 전송된 실제 네트워크 와이어 바이트
  - `zstd_savings_%` & `pubsub_savings_%`: 이벤트별 50~80% 페이로드 압축 및 오프로드 시 99.8% 브로커 절감률 확인

### 4. CLI 검증 명령어 (원격/터미널 시연 시)
```bash
.venv/bin/python3 -c "
from src.gcp_client import GCPClientFactory, GCPMode
from src.publisher import DualPathPublisher
from src.generator import SyntheticWorkloadGenerator
from src.metrics import MetricsCollector

client = GCPClientFactory.get_client(GCPMode.MOCK)
pub = DualPathPublisher(client, 'demo-topic', 'demo-bucket', offload_threshold_bytes=50*1024)
gen = SyntheticWorkloadGenerator(pub, large_payload_pct=20.0)
metrics = MetricsCollector()

results = gen.generate_batch(10)
for r in results:
    metrics.record_path(r.path.value, r.uncompressed_bytes, r.compressed_bytes, r.pubsub_wire_bytes)

counters = metrics.get_path_counters()
print('=== 이중 경로(Dual-Path) 네트워크 절감 2대 축 검증 ===')
print(f'1. 총 원본 페이로드 볼륨:          {counters[\"total_uncompressed_bytes\"] / 1024:.1f} KB')
print(f'2. Zstd 압축 후 데이터 볼륨:      {counters[\"total_compressed_bytes\"] / 1024:.1f} KB (절감률: {counters[\"overall_savings_percent\"]}%)')
print(f'3. Pub/Sub 브로커 실제 와이어 볼륨: {counters[\"total_pubsub_wire_bytes\"] / 1024:.1f} KB (절감률: {counters[\"pubsub_wire_savings_percent\"]}%)')
"
```
**출력 결과**:
```text
=== 이중 경로(Dual-Path) 네트워크 절감 2대 축 검증 ===
1. 총 원본 페이로드 볼륨:          116.9 KB
2. Zstd 압축 후 데이터 볼륨:      58.1 KB (절감률: 50.3%)
3. Pub/Sub 브로커 실제 와이어 볼륨: 3.8 KB (절감률: 96.8%)
```

### 5. 핵심 스피킹 포인트
> *"대형 바이너리가 들어와도 Pub/Sub 브로커 한도(10MB)를 절대 초과하지 않으며, GCS 오프로드를 통해 브로커 네트워크 부하를 99% 이상 절감합니다. 동시에 Zstandard 압축을 통해 페이로드 크기 자체를 50~80% 줄이고, 다운스트림 서비스는 GCS에 갔는지 인라인인지 신경 쓸 필요 없이 동일한 Protobuf 모델로 투명하게 객체를 복원합니다."*

---

## 🚀 세션 3: 지연 시간 88% 절감 — StreamingPull vs Sync Pull (Tab 3)

### 1. 배경 설명 및 아키텍처 비교
* **공식 가이드**: [Google Cloud Pub/Sub Pull Message Flow](https://docs.cloud.google.com/pubsub/docs/pull)
* Google Cloud Pub/Sub의 메시지 수급 체계는 **네트워크 커넥션 수립 방식과 주도권의 위치**에 따라 명확한 기술적 차이를 보입니다:
  1. **Sync Pull (단방향 동기식 폴링)**:
     - **메커니즘**: 클라이언트가 브로커(서버)를 향해 주기적으로 데이터 유무를 확인하는 단일 RPC 요청-응답 구조.
     - **레이턴시 패널티**: 메시지가 없을 때도 요청을 반복해야 하며, 다음 주기까지 발생하는 대기 시간과 매번 핸드셰이크를 수행하는 네트워크 오버헤드로 인해 P99 지연 시간(~140ms)이 누적됨.
     - **제어 최적화**: 수신 측의 가용 리소스 상황에 맞춰 메시지 인입량을 엄격히 제한할 수 있어 부하 관리가 용이함.
  2. **StreamingPull (영구 양방향 gRPC 스트리밍)**:
     - **메커니즘**: HTTP/2 기반의 gRPC를 활용하여 클라이언트와 서버 사이에 항시 유지되는 양방향 채널을 구축. 브로커는 데이터 인입 즉시 클라이언트 요청 없이도 실시간으로 밀어냄(Push-like).
     - **퍼포먼스 우위**: 폴링에 따르는 유휴 대기 시간이 전무하여 극도의 처리량과 P99 초저지연(~18ms)을 보장. (Anthropic이 모델 서빙 레이턴시를 88% 절감한 핵심 기술).
     - **리소스 오버헤드**: 스트림 유지 및 비동기 처리를 위한 백그라운드 프로세싱(스레드/Goroutine)이 필수적이며 메모리와 네트워크 대역폭이 상시 점유됨.

### 2. 발표자 액션 & 시연
1. 좌측의 `1. 동기식 Sync Pull 배치 실행 (10건)` 버튼 클릭.
2. 우측의 `2. gRPC StreamingPull 스트림 실행 (10건)` 버튼 클릭.

### 3. 기대되는 화면 결과
* **정보 배너**:
  - `1. Sync Pull로 10건 수신 완료. P99 지연 시간: ~140.0ms`
  - `2. StreamingPull로 10건 수신 완료. P99 지연 시간: ~18.0ms (P99 88% 단축)`
* **실시간 통계 메트릭 (P99 SLA 기준)**:
  - `1. Sync Pull P99 지연 시간`: **~140.0 ms** [태그: `P99 레거시 기준`]
  - `2. StreamingPull P99 지연 시간`: **~18.0 ms** [태그: `-88.0% (P99)`]
  - `P99 지연 시간 절감률`: **-87.1% ~ -88.0%** [태그: `-88.0% (P99)`]
  - `P99 꼬리 지연 단축 폭`: **~122.0 ms** 단축 [태그: `P99 SLA 대기 단축`]
* **막대 차트**: P99 (SLA 기준), P95, P90, P50 구간별로 좌측 1. Sync Pull 대비 우측 2. StreamingPull이 최대 8~10배 압도적인 레이턴시 안정성을 보이는 분포가 시각화됨.

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
sync_w = SyncPullWorker(client, 'p', 's1', 'bench-topic', simulated_poll_delay_ms=140.0)
for p in sync_w.pull_batch(10):
    metrics.record_latency('sync', p.latency_ms)

# StreamingPull 테스트
gen.generate_batch(10)
stream_w = StreamingPullWorker(client, 'p', 's2', 'bench-topic', callback=lambda m: metrics.record_latency('stream', m.latency_ms), simulated_stream_delay_ms=18.0)
stream_w.start()
time.sleep(0.1)
stream_w.stop()

comp = metrics.compare('sync', 'stream')
print(f'1. Sync P99: {metrics.get_stats(\"sync\")[\"p99\"]:.1f}ms')
print(f'2. Streaming P99: {metrics.get_stats(\"stream\")[\"p99\"]:.1f}ms')
print(f'P99 지연 시간 절감률: {comp[\"reduction_percent\"]:.1f}%')
"
```
**출력 결과**:
```text
1. Sync P99: 140.0ms
2. Streaming P99: 18.0ms
P99 지연 시간 절감률: 87.1%
```

### 5. 핵심 스피킹 포인트
> *"실제 대규모 LLM 서빙 및 모델 추론 환경에서는 절반의 요청만 빠른 P50(중앙값)은 의미가 없습니다. Anthropic이 주목한 진짜 핵심은 상위 1%의 최악 지연 시간을 나타내는 **P99 꼬리 지연 시간(Tail Latency)**이며, StreamingPull은 바로 이 P99 레이턴시를 88% 줄여 실시간 스트리밍 SLA를 보장합니다."*

### 6. StreamingPull이 만능 해결책인가? (전략적 Sync Pull 선택 워크로드 3가지)
* 실시간 스트리밍 관점에서는 StreamingPull이 압도적인 성능을 보이지만, 모든 인프라 환경과 비즈니스 로직에서 항상 정답인 것은 아닙니다.
* Google Cloud 가이드에서도 범용적인 비동기 처리에는 StreamingPull을 지향하도록 권장하나, **지연 시간 최적화보다 인프라 효율성이나 단순한 제어가 우선되는 특수 환경에서는 Sync Pull이 전략적으로 선택**됩니다:
  1. **배치 처리 및 서버리스 인프라 (Scale to 0)**:
     - 특정 시점에만 활성화되어(Scale to 0) 정해진 분량의 데이터를 소화하고 소멸하는 Cloud Functions나 배정된 작업만 수행하는 배치형 워크로드에는 고정적인 스트림 유지가 비효율적입니다. 필요한 시점에만 연결하는 폴링 방식이 리소스 비용 면에서 유리합니다.
  2. **하드웨어 리소스 제약 환경**:
     - 지속적인 CPU 사이클과 메모리 점유가 부담스러운 극도로 제한된 컴퓨팅 환경(IoT/Edge 기기, 경량 마이크로 컨테이너)에서는, 명시적인 요청 시에만 작동하는 동기식 모델이 시스템 안정성을 확보하기에 적합합니다.
  3. **고정밀 유량 제어 (Strict Flow Control)**:
     - 메시지 한 건당 수 분의 연산 시간이 소요되어 클라이언트 버퍼링 없이 순차적으로 엄밀하게 처리해야 하는 복잡한 로직의 경우, 직접 주도권을 갖는 Sync Pull이 구조적 단순성을 제공합니다.

---

## 🛡️ 세션 4: 데이터 전송 포맷 최적화 (바이너리 스키마) & DLQ 거버넌스 (Tab 4)

---

### 📊 세션 4-A: 데이터 전송 포맷 & 프로토콜 최적화 (Binary Schema & gRPC)

#### 1. 문제 배경 및 데이터 크기 (Size) 관점의 차이
1. **데이터 크기 (Size) 관점의 차이**:
   - 가장 큰 차이는 **데이터 필드명의 반복 여부**와 **인코딩 방식**에서 발생합니다.
   - **JSON (텍스트 포맷)**: 텍스트 기반이라 가독성은 뛰어나지만, `"event_id"`, `"timestamp_ms"`, `"pod_env_vars"`와 같은 키(Key) 문자열이 모든 단일 메시지마다 반복적으로 포함되어야 합니다. 이로 인해 불필요한 데이터가 누적되어 전체 페이로드 크기가 커집니다.
   - **Protobuf (바이너리 스키마)**: 텍스트 필드명 대신 1바이트 크기의 숫자인 Varint 태그 번호를 사용하여 필드를 식별합니다. 이에 더해 데이터를 컴팩트한 바이너리로 인코딩하므로 구조적으로 크기가 훨씬 작습니다.
2. **⚠️ 전송 프로토콜 선택 (REST vs gRPC) - Base64 +33% 패널티의 함정**:
   - 바이너리 포맷을 전송할 때는 **전송 프로토콜의 선택**이 매우 중요합니다.
   - 만약 **HTTP REST API**(`pubsub.googleapis.com/...:publish`)를 사용하면, HTTP 본문 규격(JSON)에 맞추기 위해 메시지 데이터가 **Base64로 강제 인코딩**됩니다.
   - Base64 인코딩은 3바이트를 4문자로 변환하므로 **용량이 대략 33.3% 추가 증가하는 심각한 패널티**가 발생합니다.
   - 반면 **gRPC(HTTP/2)**는 바이너리 프레이밍을 지원하므로 **순수 raw 바이너리를 그대로 전송(Base64 패널티 0%)**합니다.
3. **클라이언트단 압축(Zstd)과의 결합**:
   - 바이너리 스키마(Protobuf) + 순수 gRPC 전송 + Zstandard 압축을 결합하면, 수백 바이트 미만의 미세 데이터가 아닌 한 **단일 페이로드당 최적화할 수 있는 가장 압축된 바이트 볼륨(최대 80% 이상 절감)**을 달성하여 네트워크 Egress 및 클라우드 인프라 비용을 극적으로 낮출 수 있습니다.

#### 2. 발표자 액션 & 시연
1. Streamlit 대시보드의 **`🛡️ 3. 데이터 포맷 최적화 & DLQ`** 탭 클릭.
2. `시뮬레이션 페이로드 설정`에서 **`LLM 서빙 프롬프트/응답 (텍스트)`** 또는 **`멀티턴 에이전트 실행 컨텍스트 (긴 텍스트)`** 선택.
3. 화면 우측의 **4대 메트릭 카드 및 5대 전송 방식 비교 막대 차트** 확인.
4. `5대 전송 방식 종합 비교표`와 하단의 **`초대규모 트래픽 비용 절감 추정치 (월 10억 건 발행 기준)`** 콜아웃 박스를 가리키며 스피칭.

#### 3. 기대되는 화면 결과
* **메트릭 카드 실측치**:
  - `1. Plain JSON`: ~720 B
  - `2. JSON (REST Base64)`: ~1,000 B (+280 B, **+33% Base64 인플레이션 패널티**)
  - `4. Protobuf (gRPC)`: ~490 B (**-32% 절감**, Base64 패널티 0%)
  - `5. Protobuf+Zstd (gRPC)`: ~290 B (**-60%~-80% 최종 절감**, Anthropic 프로덕션 최적화)
* **월 10억 건(1 Billion Events) 전송 시 예상 절감량**:
  - 기존 Plain JSON 월간 트래픽: **~0.67 TB**
  - Protobuf + Zstd (gRPC) 월간 트래픽: **~0.27 TB**
  - 🎯 **순수 절감 네트워크 대역폭 및 비용: ~0.40 TB 절감 (약 60% 이상 비용 절감)**

#### 4. 터미널 CLI 벤치마크 실행 명령어
```bash
.venv/bin/python3 -c "
from src.format_benchmark import DataFormatBenchmark

bench = DataFormatBenchmark(compression_level=3)
sample = 'Explain the internal architecture of Google Cloud Pub/Sub and StreamingPull RPCs. ' * 5
res = bench.benchmark_event('evt-bench', 'claude-pod', sample)

print('=== 데이터 전송 포맷 및 프로토콜 벤치마크 결과 ===')
for r in res['results']:
    penalty = f'(Base64 패널티: +{r.base64_overhead_bytes}B)' if r.base64_overhead_bytes > 0 else '(Base64 패널티: 0B)'
    print(f'{r.format_name:<35} | {r.wire_bytes:>5} 바이트 | {r.reduction_vs_json_pct:>+6.1f}% {penalty}')

sav = res['savings_summary']
print(f'\n🎯 월 10억 건 발행 시 절감량: {sav[\"saved_tb_per_1b\"]} TB 절감 ({sav[\"overall_reduction_pct\"]}%)')
"
```
**출력 결과**:
```text
=== 데이터 전송 포맷 및 프로토콜 벤치마크 결과 ===
1. Plain JSON (텍스트)                |   612 바이트 |   +0.0% (Base64 패널티: 0B)
2. JSON over REST (Base64)          |   856 바이트 |  -39.9% (Base64 패널티: +244B)
3. Protobuf over REST (Base64)      |   672 바이트 |   -9.8% (Base64 패널티: +192B)
4. Protobuf over gRPC (순수 바이너리) |   480 바이트 |  +21.6% (Base64 패널티: 0B)
5. Protobuf + Zstd over gRPC (Anthropic) |   246 바이트 |  +59.8% (Base64 패널티: 0B)

🎯 월 10억 건 발행 시 절감량: 0.34 TB 절감 (59.8%)
```

---

### 💰 세션 4-B: 1KB 최소 과금 크기(Billing Unit) 우회: 클라이언트 측 배치(Batching) 적용

#### 1. 문제 배경 및 최적화 원리
1. **Google Cloud Pub/Sub 1KB 최소 과금 규칙**:
   - Pub/Sub은 개별 메시지 크기가 1KB(1,000바이트) 미만이더라도 **무조건 최소 1,000바이트(1KB)로 반올림하여 과금**합니다.
   - 예: 100바이트짜리 미세 메시지 10개를 각각 단일 요청으로 보내면:
     * 실제 전송 데이터: **1KB (1,000바이트)**
     * Pub/Sub 과금 처리: **10KB (10,000바이트)**
     * 결과: **무려 10배(1,000%)의 불필요한 과금 낭비 발생!**
2. **최적화 방안: 클라이언트 라이브러리 `BatchSettings` 튜닝**:
   - 메시지를 단건으로 즉시 발행하지 않고, 비즈니스 허용 지연 시간(Latency Budget: 예 50ms) 내에서 `BatchSettings(max_messages, max_bytes, max_latency)`를 구성하여 여러 메시지를 하나의 `PublishRequest`로 묶어서 전송합니다.
   - 메시지 10개를 하나로 묶어 전송하면 총 데이터 크기가 1KB(1,000바이트)로 처리되어 **과금 단위도 1KB로 정상화(최대 90% 비용 절감)**됩니다.
3. **핵심 원리 & 물리 한도 (3대 OR 조건)**:
   - Pub/Sub 클라이언트는 메시지를 버퍼링하다가 `max_messages`(최대 1,000개), `max_bytes`(최대 10MB), `max_latency` 중 **어느 하나라도 먼저 충족(OR 조건)**되면 즉시 단일 RPC로 발송합니다.
   - **기본값의 함정(Warning)**: 기본값(1ms, 1KB)은 지연 시간에만 치우쳐 개별 RPC가 폭증하므로, 프로덕션에서는 반드시 `500~1,000개`, `4MB~8MB`, `30~50ms`로 튜닝해야 합니다.
4. **프로덕션 4대 체크리스트 (Hard-Won Production Rules)**:
   - **10MB 한도 오버헤드 주의**: 서버 한계 10,000,000B 대비 gRPC 프레이밍 헤더 오버헤드를 고려하여 `max_bytes`는 반드시 **8MB ~ 9.5MB**로 설정.
   - **FlowControl 설정 누락 시 OOM 방지**: 네트워크 지연 시 미완료 Future 누적으로 인한 메모리 고갈을 막기 위해 `LimitExceededBehavior.BLOCK` 필수 적용.
   - **Graceful Shutdown 구현**: 프로세스 종료(SIGTERM) 시 버퍼 잔여 메시지 유실 방지를 위해 `futures.wait()` / `publisher.shutdown()` 필수 호출.
   - **Ordering Key 주의점**: 동일 키 메시지 순서 보장 중 단일 배치 실패 시 후속 메시지 영구 블록 방지 대책(`resume_publish`) 마련.

#### 2. 발표자 액션 & 시연
1. Streamlit 대시보드 **`🛡️ 3. 데이터 포맷 최적화 & DLQ`** 탭의 **`💰 2. 1KB 최소 과금 단위 우회 시뮬레이터`** 섹션으로 스크롤.
2. `개별 메시지 크기`를 `100B`, `월간 메시지 발행 건수`를 `10,000,000건(10M)`으로 설정.
3. `배치 묶음 메시지 수 (max_messages)`를 `1`(비배치)에서 `10`(배치)으로 이동하며 우측 메트릭의 변화 시연:
   - `비배치 과금 크기`: 9.54 GB (10.0x 비용 팽창)
   - `배치 적용 과금 크기`: 0.95 GB (**-90.0% 절감!**)
4. 하단의 `핵심 원리 및 물리 한도`, `워크로드별 권장 설정 가이드`, `프로덕션 운영 시 주의사항 (Watch-out)` 표를 가리키며 엔터프라이즈 프로덕션 환경의 권장 설정과 장애 방지 요령 설명. (상세 가이드: `docs/BATCH_TUNING_GUIDE.md`)

#### 3. CLI 배치 과금 시뮬레이션 명령어
```bash
.venv/bin/python3 -c "
from src.batch_billing import BatchBillingOptimizer

calc = BatchBillingOptimizer.calculate_billing(message_size_bytes=100, message_count=10_000_000, batch_size=10)

print('=== Pub/Sub 1KB 최소 과금 단위 및 BatchSettings 분석 ===')
print(f'개별 메시지 크기: {calc.message_size_bytes} 바이트 | 총 메시지 수: {calc.message_count:,} 건')
print(f'1. 실제 순수 데이터 크기:   {calc.actual_data_bytes / (1024**2):.2f} MB')
print(f'2. 비배치 과금 청구 크기:   {calc.unbatched_billed_bytes / (1024**2):.2f} MB (최소 1,000B 올림 적용)')
print(f'3. 배치(10개) 과금 청구 크기: {calc.batched_billed_bytes / (1024**2):.2f} MB (1KB 단위로 합산)')
print(f'🎯 과금 팽창 배수: {calc.cost_inflation_ratio}배 낭비 -> 배치 적용 시 {calc.savings_percentage}% 비용 절감!')
"
```
**출력 결과**:
```text
=== Pub/Sub 1KB 최소 과금 단위 및 BatchSettings 분석 ===
개별 메시지 크기: 100 바이트 | 총 메시지 수: 10,000,000 건
1. 실제 순수 데이터 크기:   953.67 MB
2. 비배치 과금 청구 크기:   9536.74 MB (최소 1,000B 올림 적용)
3. 배치(10개) 과금 청구 크기: 953.67 MB (1KB 단위로 합산)
🎯 과금 팽창 배수: 10.0배 낭비 -> 배치 적용 시 90.0% 비용 절감!
```

---

### 🛡️ 세션 4-C: Proto-First 거버넌스 & Dead Letter Queue (DLQ) 격리

#### 1. 배경 설명
* 수천 명의 엔지니어와 에이전트가 이벤트를 발행할 때, 단 하나의 잘못된 스키마 변경이나 손상된 바이너리(Poison Pill)가 전체 파이프라인을 멈추게 할 수 있습니다(Head-of-Line Blocking).
* Anthropic은 엄격한 Protocol Buffers 스키마와 **정확히 5회의 재시도 후 DLQ로 격리하는 서킷 브레이커**를 운영합니다.

#### 2. 발표자 액션 & 시연
1. `🚨 포이즌 필 / 스키마 손상 이벤트 주입` 버튼 클릭.

#### 3. 기대되는 화면 결과
* **에러 알림 배너**:
  - `이벤트 evt-corrupt-...가 5회 전송 실패 후 Dead Letter Queue로 안전하게 격리되었습니다!`
* **DLQ 격리 모니터링 테이블**:
  - `event_id`: `evt-corrupt-...`
  - `attempts`: `5` (최대 시도 한도 도달)
  - `status`: `dead_lettered`
  - `quarantine_reason`: `Corrupted event detected: evt-corrupt-...`
  - `timestamp`: 격리된 정확한 시각 기록

#### 4. CLI 격리 시뮬레이션 명령어
```bash
.venv/bin/python3 -c "
from src.gcp_client import GCPClientFactory, GCPMode, PublishedMessage
from src.dlq import DLQManager
from src.consumer import DualPathConsumer

client = GCPClientFactory.get_client(GCPMode.MOCK)
dlq = DLQManager(client, 'main-topic', 'dlq-topic', max_delivery_attempts=5)
consumer = DualPathConsumer(client)

# 고의로 손상된 바이너리 메시지 생성
bad_msg = PublishedMessage(message_id='bad-001', data=b'MALFORMED_GARBAGE', attributes={'content-encoding': 'zstd'})

for attempt in range(1, 6):
    res = dlq.process_with_dlq(bad_msg, consumer.consume_message)
    print(f'시도 #{res[\"attempts\"]}: 상태={res[\"status\"]}')
"
```
**출력 결과**:
```text
시도 #1: 상태=retry
시도 #2: 상태=retry
시도 #3: 상태=retry
시도 #4: 상태=retry
시도 #5: 상태=dead_lettered
```

#### 5. 핵심 스피킹 포인트
> *"바이너리 스키마(Protobuf)와 gRPC, Zstd를 결합하여 데이터 크기를 60% 이상 축소하고 HTTP REST의 Base64 +33% 팽창 패널티를 원천 차단합니다. 아울러 손상된 페이로드가 정상 컨슈머를 마비시키지 않도록 정확히 5회 실패 후 DLQ 토픽(`pubsub-demo-dlq-topic`)으로 격리하여 파이프라인 가용성(SLO 99.99%)을 달성합니다."*

---

## ☁️ 세션 5: 실제 Google Cloud 프로젝트 라이브 검증 (pub-sub-kamo)

실제 Google Cloud 프로젝트(`pub-sub-kamo`)에서 5대 아키텍처 항목을 직접 배포하고 검증할 때 사용하는 실전 가이드입니다.

### 1. 인프라 프로비저닝 (Setup Infrastructure)
> **💡 사전 안내**: Pub/Sub 서비스 에이전트에 Dead Letter Topic 및 구독에 대한 IAM 권한을 바인딩하려면, 먼저 대상 리소스(토픽, 구독, GCS 버킷, BigQuery 테이블)가 프로젝트 상에 생성되어 있어야 합니다.

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

### 2. 사전 권한 점검 및 IAM 설정 (Pre-requisites & IAM)
인프라 리소스가 생성된 후, Pub/Sub이 **Dead Letter Topic(`pubsub-demo-dlq-topic`)으로 메시지를 우회 격리**하고 **BigQuery로 Zero-ETL 스트리밍 쓰기**를 수행할 수 있도록 Google 관리형 Pub/Sub 서비스 에이전트에 필수 IAM 역할을 부여합니다.

```bash
PROJECT_ID="pub-sub-kamo"
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

# 1. Dead Letter Topic 발행 권한 부여
gcloud pubsub topics add-iam-policy-binding pubsub-demo-dlq-topic \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/pubsub.publisher"

# 2. 메인 구독 메시지 확인(Ack) 권한 부여
gcloud pubsub subscriptions add-iam-policy-binding pubsub-demo-stream-sub \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/pubsub.subscriber"

# 3. BigQuery 테이블 쓰기 권한 부여 (Zero-ETL)
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/bigquery.dataEditor"
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
• 1. Sync Pull (Batch Polling) P99 Latency: 140.2 ms
• 2. StreamingPull (Persistent gRPC) P99 Latency: 18.0 ms
✓ Measured P99 Latency Drop: 87.2% (Anthropic target: ~88% reduction achieved)

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
