def test_correct_workflow_import():
    import dags.cron_hdfs_uploader_clickstream_hits as workflow
    assert workflow is not None