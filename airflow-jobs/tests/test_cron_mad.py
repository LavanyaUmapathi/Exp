def test_correct_workflow_import():
    import dags.cron_mad as workflow
    assert workflow is not None