package com.rackspace.volga.etl.mapreduce.lib.output;

import com.rackspace.volga.etl.mapreduce.lib.io.IOConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * User: alex.silva
 * Date: 9/27/13
 * Time: 3:53 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class CSVOutputConfig implements IOConfig {

    public static final String DEFAULT_TEXTOUTPUTFORMAT_SEPARATOR = ",";

    private static final Logger LOG = LogManager.getLogger(CSVOutputConfig.class);

    @Override
    public void configure(Job job) throws IOException {
        String separator = job.getConfiguration().get("sp", DEFAULT_TEXTOUTPUTFORMAT_SEPARATOR);

        LOG.info("TextOutputFormat separator: '" + separator + "'");

        String path = job.getConfiguration().get("mapreduce.output.dir");

        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("No output path specified.");
        }

        LOG.info("Output path is: " + path);

        final Path outputPath = new Path(path);
        outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);
        CSVOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(CSVOutputFormat.class);
    }

    public String getName() {
        return "csv";
    }
}
