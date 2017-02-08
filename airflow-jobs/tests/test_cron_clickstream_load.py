def test_correct_workflow_import():
    import dags.cron_clickstream_load as workflow
    assert workflow is not None