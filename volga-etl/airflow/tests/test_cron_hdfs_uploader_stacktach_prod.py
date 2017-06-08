def test_correct_workflow_import():
    import dags.cron_hdfs_uploader_stacktach_prod as workflow
    assert workflow is not None