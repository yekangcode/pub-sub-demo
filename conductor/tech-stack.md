# Technology Stack: Google Cloud Pub/Sub Enterprise Architecture Demo

## 1. Programming Language & Runtime
- **Language**: Python 3.11+
- **Rationale**: Google Cloud 공식 클라이언트 SDK와의 최적 호환성, 비동기(asyncio) 및 gRPC StreamingPull 지원, 직관적인 데모 코드베이스 구축 가능.

## 2. Cloud Infrastructure & SDKs
- **GCP Project**: `pub-sub-kamo`
- **Google Cloud Services**:
  - `google-cloud-pubsub`: 핵심 메시징 브로커, StreamingPull(gRPC) 및 동기식 Pull, Dead Letter Queue(DLQ), 스키마 등록 지원
  - `google-cloud-storage`: 8MB 초과 대용량 페이로드(멀티모달 이미지/임베딩) 오프로드 및 GCS Pub/Sub 알림 연동
  - `google-cloud-bigquery`: Pub/Sub Push 구독을 통한 Zero-ETL 스트리밍 적재 및 실시간 검증
- **Infra Management**: Python SDK 기반 원클릭 프로비저닝 및 정리 스크립트 (`setup_infra.py`, `cleanup_infra.py`)

## 3. Serialization & Compression
- **Protobuf (Protocol Buffers v3)**: 단일 `.proto` 파일 기반의 타입 안전한 이벤트 정의 및 스키마 검증
- **zstandard (`zstd`)**: LLM 데이터(토큰/JSON/텍스트)에 대해 60~80% 압축률 달성, 99% 이상 메시지를 10MB 인라인으로 유지

## 4. UI & Visualization
- **Streamlit**: 실시간 트래픽 제어, Fast Path vs GCS 분기 카운터, 지연시간 비교(88% 절감 효과) 차트, BigQuery 적재 테이블 뷰 제공
- **Altair / Plotly**: 실시간 P50/P95/P99 지연시간 분포 차트

## 5. Testing & Quality
- **Testing**: `pytest`, `pytest-asyncio`
- **Code Quality**: `ruff` (린팅 및 포매팅)
