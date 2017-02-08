def test_correct_workflow_import():
    import dags.cron_clickstream_schema_load as workflow
    assert workflow is not None