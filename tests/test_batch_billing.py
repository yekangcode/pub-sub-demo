"""Unit tests for BatchBillingOptimizer."""

from src.batch_billing import BatchBillingOptimizer


def test_batch_billing_100b_messages_tenfold_penalty():
    # 100 bytes message, 10 messages
    # Unbatched: 10 * 1,000 bytes = 10,000 bytes billed
    # Batched (10 msgs per batch): 1 batch of 1,000 bytes = 1,000 bytes billed (1KB)
    # Inflation ratio = 10.0x!
    res = BatchBillingOptimizer.calculate_billing(
        message_size_bytes=100,
        message_count=10,
        batch_size=10,
    )

    assert res.actual_data_bytes == 1000
    assert res.unbatched_billed_bytes == 10000
    assert res.batched_billed_bytes == 1000
    assert res.cost_inflation_ratio == 10.0
    assert res.savings_percentage == 90.0


def test_batch_billing_large_messages_no_penalty():
    # 2,000 bytes message (>1,000 bytes), no rounding penalty
    res = BatchBillingOptimizer.calculate_billing(
        message_size_bytes=2000,
        message_count=100,
        batch_size=10,
    )

    assert res.actual_data_bytes == 200000
    assert res.unbatched_billed_bytes == 200000
    assert res.batched_billed_bytes == 200000
    assert res.cost_inflation_ratio == 1.0
    assert res.savings_percentage == 0.0


def test_batch_settings_code_snippet():
    snippet = BatchBillingOptimizer.get_batch_settings_code_snippet(
        max_messages=50, max_bytes_mb=2, max_latency_ms=100
    )
    assert "max_messages=50" in snippet
    assert "max_bytes=2 * 1024 * 1024" in snippet
    assert "max_latency=0.100" in snippet
