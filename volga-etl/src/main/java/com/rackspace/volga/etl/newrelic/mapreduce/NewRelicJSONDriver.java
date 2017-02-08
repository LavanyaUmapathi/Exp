package com.rackspace.volga.etl.newrelic.mapreduce;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.reflect.Reflection;
import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.mapreduce.mr.GenericETLDriver;
import com.rackspace.volga.etl.newrelic.dto.json.Monitor;

public class NewRelicJSONDriver extends GenericETLDriver {

    @Override
    protected void setUpJob(Job job) {
        Configuration conf = job.getConfiguration();
        String eventType = job.getConfiguration().get(ETLConstants.RECORD_TYPE_KEY);
        String pkgName = Reflection.getPackageName(Monitor.class);
        String className = pkgName + "." + StringUtils.camelize(eventType);
        conf.set(ETLConstants.DTO_CLASS_KEY, className);
        super.setUpJob(job);
    }

    @Override
    protected Option[] getExtraOptions() {
        return new Option[]{
                new Option("et", ETLConstants.RECORD_TYPE_KEY, true,
                        "The new relic record type:monitor,poll.")};
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new NewRelicJSONDriver(), args));
    }

}
