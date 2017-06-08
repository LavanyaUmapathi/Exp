def test_correct_workflow_import():
    import dags.hdfs_status_check as workflow
    assert workflow is not None
