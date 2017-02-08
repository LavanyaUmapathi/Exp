def test_correct_workflow_import():
    import dags.cron_mnl as workflow
    assert workflow is not None