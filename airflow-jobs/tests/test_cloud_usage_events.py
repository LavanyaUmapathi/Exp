def test_cloud_usage_events_import():
    import importlib
    workflow = importlib.import_module("dags.cloud-usage-events")
    assert workflow is not None
