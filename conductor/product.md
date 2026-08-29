# Product Definition: Google Cloud Pub/Sub Enterprise Architecture Demo

## 1. Overview
Google Cloud Pub/Sub을 기반으로 대규모 AI/LLM 워크로드에서 요구되는 고성능 스트리밍 데이터 파이프라인 아키텍처를 시각적으로 시연하는 엔터프라이즈 고객용 데모 프로젝트입니다. Anthropic의 대규모 프로덕션 사례에서 검증된 핵심 패턴들을 순수 Google Cloud 환경(GCP 프로젝트: `pub-sub-kamo`)에서 실시간으로 재현하고 가치를 입증합니다.

## 2. Target Audience & Scope
- **대상 고객**:
  - 엔터프라이즈 AI 및 데이터 아키텍트 (대규모 LLM/Agentic AI 서빙 데이터의 신뢰성, 안전성 파이프라인 관심군)
  - 클라우드 인프라 및 DevOps 엔지니어 (완전 관리형 Zero-Ops 브로커, 스케일링, 인프라 자동화 관심군)
- **범위**: 
  - 100% Google Cloud 네이티브 환경 (GCP 프로젝트: `pub-sub-kamo`)
  - 외부/멀티클라우드는 배제하고 Google Cloud의 핵심 관리형 서비스 간의 유기적 통합에 집중

## 3. Key Value Propositions
- **Zero-Ops & 무중단 자동 스케일링**: Kafka/ZooKeeper 클러스터 운영 및 파티션/디스크 사이징 오버헤드 없이 수만~수백만 건의 버스트 트래픽을 처리.
- **비용 및 페이로드 최적화**: zstd 압축과 8MB 임계치 기반 GCS 오프로드를 통해 네트워크 전송 비용 절감 및 99% 이상 메시지의 초고속 인라인 처리.
- **극단적인 지연 시간 단축**: 동기식 Pull의 빈 응답 대기 오버헤드를 gRPC 양방향 StreamingPull로 전환하여 유효 처리 시간 97%+ 달성 및 대기 지연 88% 제거.
- **셀프서비스 거버넌스 (Zero-ETL)**: 개발자가 `.proto` 스키마만 정의하면 Pub/Sub 토픽, 스키마 검증, BigQuery 분석 테이블, Zero-ETL Push 구독, DLQ가 자동 프로비저닝됨.

## 4. Core Demo Modules
### Module 1: 대규모 데이터 수집과 "이중 경로(Dual-Path)" 패턴
- **합성 트래픽 생성기**:
  - 단문 프롬프트/응답(수 KB), 긴 대화 컨텍스트/도구 호출(수십 KB~수 MB), 대형 이미지/임베딩/가중치 텐서(8MB 초과)를 동적으로 생성.
- **이중 경로(Dual-Path) 분기**:
  - Protobuf 직렬화 후 `zstandard` (zstd) 압축 적용 (60~80% 페이로드 감축).
  - **Fast Path (< 8MB)**: Pub/Sub 토픽으로 직접 발행 -> Push/Streaming 구독 서비스가 즉시 BigQuery/Storage로 적재.
  - **Large Payload Path (>= 8MB)**: 압축 블롭을 Cloud Storage(GCS)에 업로드 -> GCS 객체 생성 알림(Pub/Sub) 발행 -> 소비자가 포인터를 받아 GCS에서 원본을 인출/처리.
- **Dead Letter Queue (DLQ)**: 실패 시 안전 격리 및 복구 파이프라인 시연.

### Module 2: 지연 시간 88% 절감 (StreamingPull 전환)
- **비교 벤치마크 모듈**:
  - **Synchronous Pull 모드**: 다수의 워커가 주기적 Long-polling HTTP 요청을 보내며 발생하는 대기 시간, 유휴 CPU 오버헤드 측정.
  - **StreamingPull 모드**: 파드당 양방향 gRPC 스트림을 영구 유지하여 메시지 즉시 수신, 동적 플로우 제어(Flow Control), 실시간 Ack/데드라인 연장 동작.
- **실시간 메트릭 비교**:
  - P50/P95/P99 종단간(E2E) 지연시간 비교.
  - 워커의 유효 CPU 처리 시간 비율 (StreamingPull 97%+ vs Synchronous Pull 저조한 유효율).

### Module 3: Proto-First 셀프서비스 이벤트 플랫폼
- **단일 .proto 스키마 기반 자동화**:
  - 이벤트 정의 및 BigQuery 프로젝트/데이터셋/파티션 메타데이터 옵션이 명시된 `.proto` 파일.
- **인프라 프로비저닝 자동화 스크립트**:
  - Protobuf 정의를 읽어 GCP Pub/Sub 스키마, 토픽, BigQuery 테이블(파티셔닝 적용), Push 구독, DLQ를 원클릭 프로비저닝.
- **BigQuery Zero-ETL 적재**:
  - 별도 스트리밍 파이프라인(ETL 서버) 없이 Pub/Sub Push 구독을 통해 BigQuery 웨어하우스로 실시간 직결 및 쿼리 확인.

## 5. UI & Delivery Formats
- **경량 웹 UI 대시보드 (Streamlit 또는 FastAPI)**:
  - 트래픽 생성 제어 (초당 메시지 수, 대형 페이로드 비율 슬라이더 조절).
  - 실시간 파이프라인 흐름도 및 Fast Path vs GCS Offload 카운터.
  - Synchronous Pull vs StreamingPull 지연시간 및 CPU 효율 비교 차트.
  - BigQuery 실시간 적재 데이터 테이블 미리보기.
- **스토리지 인프라**:
  - Google Cloud BigQuery (이벤트 분석 테이블)
  - Google Cloud Storage (대용량 블롭 및 DLQ 아카이브)
  - Google Cloud Pub/Sub (코어 메시징 레이어)
