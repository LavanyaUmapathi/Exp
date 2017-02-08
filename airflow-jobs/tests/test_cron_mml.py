def test_correct_workflow_import():
    import dags.cron_mml as workflow
    assert workflow is not None