package com.rackspace.volga.etl.mapreduce.lib.io;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 4:54 PM
 * Copyright Rackspace Hosting, Inc.
 */
public interface IOConfig {
    public void configure(Job job) throws IOException;

    String getName();
}
