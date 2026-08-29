# Specification: MVP Google Cloud Pub/Sub Enterprise Architecture Demo

## 1. Overview
Google Cloud Pub/Sub 기반의 엔터프라이즈 실시간 스트리밍 아키텍처 데모 환경(GCP 프로젝트: `pub-sub-kamo`)을 구축합니다. Anthropic의 프로덕션 패턴을 바탕으로, 대규모 AI/LLM 워크로드에서 필수적인 Dual-Path 수집, StreamingPull 지연시간 혁신(88% 절감), Proto-First BigQuery Zero-ETL 및 DLQ 신뢰성 플로우를 실시간 Streamlit 대시보드를 통해 시각적으로 입증합니다.

## 2. Functional Requirements
### F1. Proto-First 이벤트 스키마 & 직렬화
- `StreamingEvent` Protobuf v3 정의: `event_id`, `source`, `payload`, `payload_type`, `timestamp`, `pod_env_vars`, `is_corrupted`
- `zstandard` (zstd) 압축 레이어 구현 (`content-encoding: zstd`): LLM/스트리밍 데이터 60~80% 압축

### F2. 이중 경로(Dual-Path) 수집기
- **Fast Path (< 8MB)**: zstd 압축 후 8MB 미만인 경우 Pub/Sub 토픽으로 직접 발행
- **Large Payload Path (>= 8MB)**: 압축 후 8MB 이상인 경우 GCS 버킷에 업로드 후 객체 메타데이터/포인터를 Pub/Sub으로 발행
- **Ingestion Service**: Push/Streaming 구독을 통해 메시지를 수신하고, Fast Path는 인라인 복원, Large Payload는 GCS에서 원본 블롭을 인출하여 수집 완료 처리

### F3. StreamingPull vs Sync Pull 지연시간 벤치마크
- 단일 토픽에서 2개의 서브스크립션으로 트래픽을 동시 팬아웃:
  1. `sub-sync-pull`: HTTP 동기식 Pull (Long-polling 빈 응답 대기 오버헤드 측정)
  2. `sub-streaming-pull`: gRPC 양방향 StreamingPull (메시지 즉시 수신, 동적 플로우 제어)
- P50 / P95 / P99 E2E 지연시간 및 워커 CPU 유휴 시간 비율을 실시간 집계하여 88% 지연 제거 수치 도출

### F4. BigQuery Zero-ETL & DLQ 에러 격리
- Pub/Sub Push 구독을 통한 BigQuery 테이블 영구 보존(Partitioned) Zero-ETL 스트리밍 적재
- 대시보드에서 '의도적 에러 주입' 활성화 시 5회 재시도 실패 후 DLQ 토픽 및 에러 테이블로 안전하게 격리되는 흐름 시연

### F5. Streamlit 실시간 대시보드 & 모의(Mock) 모드
- 슬라이더로 초당 전송률(Rate), 대형 페이로드(>= 8MB) 생성 비율(0~20%), 에러 주입 토글 제어
- Fast Path vs GCS 분기 카운터 및 실시간 파이프라인 토폴로지 표시
- Sync Pull vs StreamingPull 지연시간 비교 실시간 바/라인 차트
- BigQuery 수집 데이터 실시간 테이블 뷰
- GCP 환경(`pub-sub-kamo`) 및 로컬 오프라인 Mock/에뮬레이터 모드 동시 지원

## 3. Non-Functional Requirements & Acceptance Criteria
- **성능 검증**: StreamingPull 워커의 유효 CPU 처리 시간 >95% 및 동기식 Pull 대비 대기 지연 80% 이상 절감 확인
- **TDD 검증**: 단위/통합 테스트 커버리지 >80% 준수
- **운영 편의성**: `setup_infra.py` (원클릭 프로비저닝) 및 `cleanup_infra.py` (원클릭 자원 정리) 제공
