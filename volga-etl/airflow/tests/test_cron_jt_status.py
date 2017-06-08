def test_correct_workflow_import():
    import dags.cron_jt_status as workflow
    assert workflow is not None