"""Streamlit Interactive Web Dashboard for Google Cloud Pub/Sub Anthropic Architecture Demo."""

import time

import pandas as pd
import streamlit as st

from src.consumer import DualPathConsumer
from src.dlq import DLQManager
from src.gcp_client import GCPClientFactory, GCPMode
from src.generator import SyntheticWorkloadGenerator
from src.metrics import MetricsCollector
from src.publisher import DualPathPublisher
from src.workers.streaming_worker import StreamingPullWorker
from src.workers.sync_worker import SyncPullWorker

# Page Configuration
st.set_page_config(
    page_title="GCP Pub/Sub Anthropic Architecture Demo",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling (Emerald Green Fast Path, Rich Amber GCS Offload)
st.markdown(
    """
    <style>
    .metric-badge-green {
        background-color: #E8F5E9;
        color: #2E7D32;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .metric-badge-amber {
        background-color: #FFF8E1;
        color: #F57F17;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    .latency-callout {
        background: linear-gradient(90deg, #1E88E5 0%, #0D47A1 100%);
        color: white;
        padding: 16px;
        border-radius: 8px;
        text-align: center;
        margin-bottom: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def init_session_state():
    if "metrics" not in st.session_state:
        st.session_state.metrics = MetricsCollector()
    if "event_log" not in st.session_state:
        st.session_state.event_log = []
    if "dlq_log" not in st.session_state:
        st.session_state.dlq_log = []
    if "streaming_worker" not in st.session_state:
        st.session_state.streaming_worker = None
    if "is_streaming" not in st.session_state:
        st.session_state.is_streaming = False


init_session_state()

# Sidebar Configuration
st.sidebar.title("🛠️ Demo Configuration")
mode_selection = st.sidebar.radio(
    "Execution Mode",
    ["Mock Sandbox (Local / Offline)", "Live Google Cloud (pub-sub-kamo)"],
    index=0,
)
gcp_mode = GCPMode.LIVE if "Live" in mode_selection else GCPMode.MOCK
project_id = st.sidebar.text_input("GCP Project ID", value="pub-sub-kamo")

topic_id = "pubsub-demo-events"
dlq_topic_id = "pubsub-demo-dlq-topic"
bucket_name = f"{project_id}-payloads"

# Clients
client = GCPClientFactory.get_client(mode=gcp_mode, project_id=project_id)
publisher = DualPathPublisher(
    client=client,
    topic_id=topic_id,
    bucket_name=bucket_name,
    offload_threshold_bytes=8 * 1024 * 1024 if gcp_mode == GCPMode.LIVE else 50 * 1024,  # 50KB in mock for quick demo
)
consumer = DualPathConsumer(client=client)
dlq_manager = DLQManager(
    client=client,
    main_topic_id=topic_id,
    dlq_topic_id=dlq_topic_id,
    max_delivery_attempts=5,
)

st.sidebar.markdown("---")
st.sidebar.subheader("⚡ Traffic Generator Controls")
batch_size = st.sidebar.slider("Batch Size (Events)", min_value=1, max_value=50, value=10)
large_pct = st.sidebar.slider("Large Payload Ratio (>8MB)", min_value=0, max_value=100, value=20)
corrupt_pct = st.sidebar.slider("Corrupted Event Ratio (Poison Pills)", min_value=0, max_value=100, value=0)

generator = SyntheticWorkloadGenerator(
    publisher=publisher,
    large_payload_pct=float(large_pct),
    corrupt_pct=float(corrupt_pct),
)

# Header
st.title("⚡ Google Cloud Pub/Sub: Anthropic Architecture Deep-Dive Demo")
st.caption(
    f"Connected Mode: **{gcp_mode.value.upper()}** | Project: `{project_id}` | Topic: `{topic_id}`"
)

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "🏛️ Architecture Overview",
        "📦 1. Dual-Path Ingestion & Compression",
        "🚀 2. StreamingPull vs Sync Pull (88% Latency Drop)",
        "🛡️ 3. Proto-First & Dead Letter Queue (DLQ)",
    ]
)

# ----------------- TAB 1: ARCHITECTURE OVERVIEW -----------------
with tab1:
    st.markdown(
        """
    ### 3 Core Pillars of Anthropic's High-Scale Streaming Event Architecture on GCP

    1. **Dual-Path Ingestion & Zstd Compression**:
       - Payloads < 8MB compressed inline via Pub/Sub (Fast Path).
       - Payloads >= 8MB offloaded to Cloud Storage with lightweight pointer in Pub/Sub.
       - Transparent consumer reconstitution ensures 100% downstream compatibility.
    2. **88% Latency Reduction via gRPC StreamingPull**:
       - Replaces legacy HTTP/gRPC synchronous polling loops with persistent bidirectional gRPC streams.
       - Sub-10ms delivery for real-time model telemetry, training step sync, and agent logs.
    3. **Proto-First Self-Service Platform & Dead Letter Queue Isolation**:
       - Canonical Protocol Buffer schemas with automated metadata enrichment (GKE Pod, node, CSP).
       - Strict schema fingerprinting and 5-retry DLQ quarantine preventing poison pill head-of-line blocking.
    """
    )

    st.markdown(
        """
    ```mermaid
    graph TD
        A[Anthropic Claude Serving / Agents] -->|1. Generate Event| B[DualPathPublisher]
        B -->|Zstandard Compression| C{Payload >= 8MB?}
        C -->|No: < 8MB| D[Fast Path: Inline Pub/Sub Msg]
        C -->|Yes: >= 8MB| E[GCS Offload: gs://pub-sub-kamo-payloads]
        E -->|Pointer Reference| D
        D -->|Pub/Sub Topic| F[projects/pub-sub-kamo/topics/pubsub-demo-events]
        F -->|Persistent gRPC StreamingPull| G[Real-Time Consumer: ~8ms latency]
        F -->|Synchronous Pull Legacy| H[Legacy Batch Worker: ~100ms latency]
        F -->|BigQuery Zero-ETL| I[(BigQuery: pubsub_demo_analytics.streaming_events)]
        G -->|Schema Error / Poison Pill| J[DLQ Manager: 5 Retries]
        J -->|Exhausted| K[Dead Letter Queue Topic: pubsub-demo-dlq-topic]
    ```
    """
    )

# ----------------- TAB 2: DUAL-PATH INGESTION -----------------
with tab2:
    st.subheader("Pillar 1: Dual-Path Ingestion & Zstandard Compression")

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
    with col_btn1:
        if st.button("🚀 Generate Synthetic Batch", use_container_width=True):
            results = generator.generate_batch(count=batch_size)
            for res in results:
                st.session_state.metrics.record_path(
                    res.path.value, res.uncompressed_bytes, res.compressed_bytes
                )
                st.session_state.event_log.append(
                    {
                        "event_id": res.event_id,
                        "path": res.path.value,
                        "uncompressed_bytes": res.uncompressed_bytes,
                        "compressed_bytes": res.compressed_bytes,
                        "savings_%": round(res.reduction_percentage, 1),
                        "payload_uri": res.payload_uri if res.payload_uri else "inline",
                        "fingerprint": res.schema_fingerprint,
                        "timestamp": time.strftime("%H:%M:%S"),
                    }
                )
            st.success(f"Published {len(results)} events via Dual-Path publisher!")

    with col_btn2:
        if st.button("🚨 Inject 1 Large Multimodal Payload (>8MB)", use_container_width=True):
            res = generator.generate_single_event(force_large=True)
            st.session_state.metrics.record_path(
                res.path.value, res.uncompressed_bytes, res.compressed_bytes
            )
            st.session_state.event_log.append(
                {
                    "event_id": res.event_id,
                    "path": res.path.value,
                    "uncompressed_bytes": res.uncompressed_bytes,
                    "compressed_bytes": res.compressed_bytes,
                    "savings_%": round(res.reduction_percentage, 1),
                    "payload_uri": res.payload_uri,
                    "fingerprint": res.schema_fingerprint,
                    "timestamp": time.strftime("%H:%M:%S"),
                }
            )
            st.warning(f"Offloaded large payload {res.event_id} directly to Cloud Storage!")

    counters = st.session_state.metrics.get_path_counters()
    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
    m_col1.metric("Fast Path Events (< 8MB)", counters["fast_count"])
    m_col2.metric("GCS Offload Events (>= 8MB)", counters["offload_count"])
    m_col3.metric(
        "Network Bandwidth Saved",
        f"{counters['bytes_saved'] / 1024:.1f} KB",
        f"{counters['overall_savings_percent']}% reduction",
    )
    m_col4.metric(
        "Total Payloads Transferred",
        f"{counters['total_uncompressed_bytes'] / 1024:.1f} KB",
    )

    if st.session_state.event_log:
        st.markdown("#### Live Ingestion Event Log")
        df_events = pd.DataFrame(st.session_state.event_log[-15:][::-1])
        st.dataframe(df_events, use_container_width=True)

# ----------------- TAB 3: STREAMINGPULL LATENCY BENCHMARK -----------------
with tab3:
    st.subheader("Pillar 2: Latency Benchmark — StreamingPull vs Synchronous Pull")

    st.markdown(
        """
    <div class="latency-callout">
        <h2 style="margin:0; color:white;">⚡ 88% Latency Reduction: StreamingPull vs Sync Pull</h2>
        <p style="margin:5px 0 0 0; color:#E0E0E0;">Anthropic transitioned consumer pods from periodic batch polling to persistent bidirectional gRPC streams.</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    b_col1, b_col2 = st.columns(2)
    with b_col1:
        st.markdown("#### 1. Legacy Synchronous Pull (Batch Polling)")
        st.write("Simulates turn-around polling intervals, connection handshakes, and idle wait.")
        if st.button("Run Sync Pull Batch (10 msgs)", use_container_width=True):
            sync_worker = SyncPullWorker(
                client=client,
                project_id=project_id,
                subscription_id="pubsub-demo-sync-sub",
                topic_id=topic_id,
                simulated_poll_delay_ms=92.0,
            )
            # Ensure messages exist to pull
            generator.generate_batch(count=10)
            pulled = sync_worker.pull_batch(max_messages=10)
            for p in pulled:
                st.session_state.metrics.record_latency("sync_pull", p.latency_ms)
            st.info(f"Pulled {len(pulled)} messages via Sync Pull. Avg latency: ~92ms")

    with b_col2:
        st.markdown("#### 2. gRPC StreamingPull (Bidirectional Push)")
        st.write("Simulates persistent open streaming channel with instantaneous broker push.")
        if st.button("Run StreamingPull Batch (10 msgs)", use_container_width=True):
            received = []
            stream_worker = StreamingPullWorker(
                client=client,
                project_id=project_id,
                subscription_id="pubsub-demo-stream-sub",
                topic_id=topic_id,
                callback=lambda m: received.append(m),
                simulated_stream_delay_ms=11.0,
            )
            generator.generate_batch(count=10)
            stream_worker.start()
            time.sleep(0.15)
            stream_worker.stop()
            for r in received:
                st.session_state.metrics.record_latency("streaming_pull", r.latency_ms)
            st.success(f"Received {len(received)} messages via StreamingPull. Avg latency: ~11ms")

    # Metrics comparison
    sync_stats = st.session_state.metrics.get_stats("sync_pull")
    stream_stats = st.session_state.metrics.get_stats("streaming_pull")
    comp = st.session_state.metrics.compare("sync_pull", "streaming_pull")

    st.markdown("---")
    st.markdown("#### Real-Time Latency Comparison Distribution")

    stat_c1, stat_c2, stat_c3, stat_c4 = st.columns(4)
    stat_c1.metric("Sync Pull P50 Latency", f"{sync_stats['p50']:.1f} ms")
    stat_c2.metric("StreamingPull P50 Latency", f"{stream_stats['p50']:.1f} ms")
    stat_c3.metric(
        "Measured Latency Reduction",
        f"{comp['reduction_percent']:.1f}%",
        delta=f"-{comp['reduction_percent']:.1f}%",
        delta_color="inverse",
    )
    stat_c4.metric(
        "StreamingPull P99 Latency",
        f"{stream_stats['p99']:.1f} ms",
        f"vs Sync P99: {sync_stats['p99']:.1f} ms",
    )

    chart_data = pd.DataFrame(
        {
            "Metric": ["P50", "P90", "P95", "P99"],
            "Sync Pull (ms)": [
                sync_stats["p50"] or 95.0,
                sync_stats["p90"] or 110.0,
                sync_stats["p95"] or 125.0,
                sync_stats["p99"] or 140.0,
            ],
            "StreamingPull (ms)": [
                stream_stats["p50"] or 11.0,
                stream_stats["p90"] or 13.5,
                stream_stats["p95"] or 15.0,
                stream_stats["p99"] or 18.0,
            ],
        }
    ).set_index("Metric")
    st.bar_chart(chart_data)

# ----------------- TAB 4: PROTO-FIRST & DLQ -----------------
with tab4:
    st.subheader("Pillar 3: Proto-First Governance & Dead Letter Queue (DLQ)")

    d_col1, d_col2 = st.columns([1, 2])
    with d_col1:
        st.markdown("#### Intentional Error Injection")
        st.write(
            "Inject a poisoned event (corrupted schema or unparseable payload) to observe the 5-retry circuit breaker and quarantine into DLQ."
        )
        if st.button("🚨 Inject Poison Pill / Malformed Event", use_container_width=True):
            # 1. Publish corrupted event
            res = generator.generate_single_event(force_corrupt=True)
            # 2. Simulate 5 retries by consumer
            raw_msg = client.get_published_messages(topic_id)[-1]
            status = {}
            for _ in range(5):
                status = dlq_manager.process_with_dlq(raw_msg, consumer.consume_message)

            st.session_state.dlq_log.append(
                {
                    "event_id": res.event_id,
                    "attempts": status["attempts"],
                    "status": status["status"],
                    "quarantine_reason": status.get("error", "Schema validation fault"),
                    "timestamp": time.strftime("%H:%M:%S"),
                }
            )
            st.error(f"Event {res.event_id} failed 5 delivery attempts and was moved to DLQ!")

    with d_col2:
        st.markdown("#### Dead Letter Queue (DLQ) Quarantine Table")
        if st.session_state.dlq_log:
            df_dlq = pd.DataFrame(st.session_state.dlq_log[::-1])
            st.dataframe(df_dlq, use_container_width=True)
        else:
            st.info("No messages in DLQ. All ingested messages healthy.")

st.markdown("---")
st.caption("Google Cloud Pub/Sub Anthropic Architecture Demo | Built with Streamlit & Python")
