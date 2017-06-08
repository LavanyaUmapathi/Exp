def test_correct_workflow_import():
    import dags.cron_mal as workflow
    assert workflow is not None