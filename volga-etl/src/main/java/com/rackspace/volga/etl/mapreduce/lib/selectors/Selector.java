package com.rackspace.volga.etl.mapreduce.lib.selectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

/**
 * User: alex.silva
 * Date: 6/4/14
 * Time: 10:32 AM
 * Copyright Rackspace Hosting, Inc.
 */
public interface Selector {
    List<FileStatus> select(Job job, Path path, int n) throws IOException;
}
