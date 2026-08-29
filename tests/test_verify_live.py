from scripts.verify_gcp_live import verify_live_deployment


def test_verify_live_deployment_dry_run():
    success = verify_live_deployment(project_id="pub-sub-kamo", dry_run=True)
    assert success is True
