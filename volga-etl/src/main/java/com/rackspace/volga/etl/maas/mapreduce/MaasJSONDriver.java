package com.rackspace.volga.etl.maas.mapreduce;

import com.google.common.reflect.Reflection;
import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import com.rackspace.volga.etl.mapreduce.mr.GenericETLDriver;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: alex.silva
 * Date: 7/23/14
 * Time: 3:13 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class MaasJSONDriver extends GenericETLDriver {

    @Override
    protected void setUpJob(Job job) {
        Configuration conf = job.getConfiguration();
        String eventType = job.getConfiguration().get(ETLConstants.EVENT_TYPE_KEY);
        String pkgName = Reflection.getPackageName(Metric.class);
        String className = pkgName + "." + StringUtils.camelize(eventType);
        conf.set(ETLConstants.DTO_CLASS_KEY, className);
        super.setUpJob(job);
    }

    @Override
    protected Option[] getExtraOptions() {
        return new Option[]{
                new Option("et", ETLConstants.EVENT_TYPE_KEY, true,
                        "The event type:metric,configuration,notification.")};
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MaasJSONDriver(), args));
    }

}
