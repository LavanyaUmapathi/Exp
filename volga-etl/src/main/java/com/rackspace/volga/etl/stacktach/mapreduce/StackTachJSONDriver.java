package com.rackspace.volga.etl.stacktach.mapreduce;

import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.mapreduce.mr.GenericETLDriver;
import com.rackspace.volga.etl.stacktach.dto.json.SystemEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: alex.silva
 * Date: 4/3/15
 * Time: 3:13 PM
 *
 * Copyright Rackspace Hosting, Inc.
 *
 * This driver uses the default transformer for StackTach as defined in dozer.xml file.
 *
 */
public class StackTachJSONDriver extends GenericETLDriver {

    @Override
    protected void setUpJob(Job job) {
        Configuration conf = job.getConfiguration();
        conf.set(ETLConstants.DTO_CLASS_KEY, SystemEvent.class.getCanonicalName());
        super.setUpJob(job);
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StackTachJSONDriver(), args));
    }

}
