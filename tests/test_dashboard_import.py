import importlib


def test_dashboard_imports_cleanly():
    """Verify streamlit dashboard imports without syntax or dependency errors."""
    mod = importlib.import_module("src.dashboard")
    assert mod is not None
