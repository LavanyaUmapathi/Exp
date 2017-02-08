package com.rackspace.volga.etl.mapreduce.lib.output;

import com.rackspace.volga.etl.mapreduce.lib.io.IOConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 9/27/13
 * Time: 3:53 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class TextOutputConfig implements IOConfig {
    private static final Logger LOG = LogManager.getLogger(TextOutputConfig.class);

    @Override
    public void configure(Job job) {

        String path = job.getConfiguration().get("mapreduce.output.dir");

        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("No output path specified.");
        }

        LOG.info("Output path is: " + path);

        final Path outputPath = new Path(path);

        try {
            outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(getOutputFormatClass());
    }

    protected Class<? extends OutputFormat> getOutputFormatClass() {
        return TextOutputFormat.class;
    }

    public String getName() {
        return "text";
    }
}
