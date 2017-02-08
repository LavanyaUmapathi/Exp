package com.rackspace.volga.etl.mapreduce.mr;

import com.rackspace.volga.etl.mapreduce.args.CommandLineBuilder;
import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import com.rackspace.volga.etl.mapreduce.lib.io.TextArrayWritable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 4:11 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class GenericETLDriver extends Configured implements Tool {

    private static final Logger LOG = LogManager.getLogger(GenericETLDriver.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GenericETLDriver(), args));
    }

    protected Option[] getExtraOptions() {
        return null;
    }

    protected void parse(String[] args, Job job) throws ParseException {
        CommandLine cmd = new CommandLineBuilder().buildCommonOptions(args, getExtraOptions());
        //copy into config
        for (Option o : cmd.getOptions()) {
            job.getConfiguration().set(o.getLongOpt(), o.getValue());
        }
    }


    public int run(String[] args) throws Exception {
        final Job job = new Job(getConf());
        job.setJarByClass(this.getClass());
        LOG.info("Setting jar by class: " + this.getClass());
        job.setJarByClass(GenericETLDriver.class);
        job.setJobName(this.getClass().getName() + "");
        parse(args, job);

        final String outputFormatName = job.getConfiguration().get("mapreduce.output.format", "csv");
        IO.setOutput(outputFormatName, job);

        final String inputFormatName = job.getConfiguration().get("mapreduce.input.format", "text");
        IO.setInput(inputFormatName, job);

        setUpJob(job);

        String reducerClassName = job.getConfiguration().get("mapreduce.reducer.class");
        boolean hasReducerClass = !StringUtils.isEmpty(reducerClassName) && !reducerClassName.equals("none");
        if (hasReducerClass) {
            Class<? extends Reducer> reducerClass = ClassUtils.getClass(reducerClassName);
            job.getConfiguration().setClass("mapreduce.reducer.class", reducerClass, GenericETLReducer.class);
        }

        LazyOutputFormat.setOutputFormatClass(job, job.getOutputFormatClass());
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    /**
     * To be overriden by subclasses.
     *
     * @param job
     */
    protected void setUpJob(Job job) {
        job.setReducerClass(GenericETLReducer.class);
        job.setMapperClass(GenericETLMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(NullWritable.class);
    }


}

