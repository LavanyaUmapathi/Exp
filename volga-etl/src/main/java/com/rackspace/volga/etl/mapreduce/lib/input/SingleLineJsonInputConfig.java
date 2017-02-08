package com.rackspace.volga.etl.mapreduce.lib.input;

import com.rackspace.volga.etl.mapreduce.lib.io.IOConfig;
import org.apache.hadoop.mapreduce.Job;

/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 4:56 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class SingleLineJsonInputConfig implements IOConfig {
    @Override
    public void configure(Job job) {
        job.setInputFormatClass(SingleLineJsonInputFormat.class);
    }

    @Override
    public String getName() {
        return "singlejson";
    }
}
