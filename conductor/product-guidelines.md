# Product Guidelines: Google Cloud Pub/Sub Enterprise Architecture Demo

## 1. Voice & Tone
- **Professional & Engineering-Driven**: 과장 없는 명확한 엔지니어링 수치(P99 지연시간, 초당 처리량, CPU 유휴 시간 비율 등)를 기반으로 아키텍처의 신뢰성을 전달합니다.
- **Enterprise-Grade**: 단순 장난감 예제가 아닌 프로덕션급 보안, DLQ 에러 핸들링, 스키마 검증 패턴을 강조합니다.
- **Developer-Friendly**: 직관적인 CLI 명령어와 가독성 높은 실시간 대시보드로 시연자가 고객 미팅에서 막힘없이 프리젠테이션할 수 있도록 지원합니다.

## 2. UX & Visualization Principles
- **시각적 경로 분기 명확성 (Dual-Path Ingestion)**:
  - Fast Path (< 8MB, zstd 압축 인라인): 신속함을 나타내는 녹색(Green) 뱃지 및 실시간 스트림 카운터 제공.
  - Large Payload Path (>= 8MB, GCS 오프로드): 스토리지 경유를 명시하는 주황색(Amber) 뱃지 및 원본 크기, GCS URI 포인터 표시.
- **지연 시간 88% 절감 효과의 직관적 대비 (Latency & Throughput Contrast)**:
  - Synchronous Pull (HTTP 롱폴링 대기) vs StreamingPull (gRPC 스트림)을 좌우 나란히(Side-by-side) 배치.
  - P50 / P95 / P99 지연시간 비교 바 차트 및 "88% Idle Latency Elimination" 하이라이트 배너 제공.
- **BigQuery Zero-ETL 검증 뷰**:
  - 데이터가 유입되는 즉시 BigQuery 테이블에 실시간 스트리밍 적재되는 과정을 데이터 테이블 뷰로 직접 확인 가능.
- **에러 및 DLQ 신뢰성 표시**:
  - 스키마 불일치 또는 고의 주입된 실패 메시지가 DLQ 토픽으로 안전하게 격리되는 흐름을 모니터링 뱃지로 표현.

## 3. Demo Operational Standards
- **원클릭 구동**: 복잡한 사전 설정 없이 단일 명령어(`run_demo.sh` 또는 `streamlit run`)로 모의 트래픽과 대시보드가 즉시 구동되어야 함.
- **GCP 리소스 안전 관리**: 데모 종료 후 생성된 토픽, 서브스크립션, 테이블, GCS 버킷을 손쉽게 정리(Clean-up)할 수 있는 자동화 스크립트 제공.
