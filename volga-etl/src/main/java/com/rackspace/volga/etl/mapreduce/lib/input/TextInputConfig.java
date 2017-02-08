package com.rackspace.volga.etl.mapreduce.lib.input;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import com.rackspace.volga.etl.mapreduce.lib.io.IOConfig;
import com.rackspace.volga.etl.mapreduce.lib.selectors.Selector;
import com.rackspace.volga.etl.mapreduce.lib.selectors.Selectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * User: alex.silva
 * Date: 9/27/13
 * Time: 3:53 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class TextInputConfig implements IOConfig {

    @Override
    public void configure(Job job) throws IOException {
        job.setInputFormatClass(TextInputFormat.class);
        final String basePath = job.getConfiguration().get("mapreduce.input.path");
        final String pattern = job.getConfiguration().get("mapreduce.input.pattern");
        final String selectorString = job.getConfiguration().get("mapreduce.input.selector");

        List<String> filePaths = getFileInputs(basePath,job);

        String inputPath = Joiner.on(",").skipNulls().join(filePaths);

        if (!StringUtils.isEmpty(pattern)) {
            job.getConfiguration().set(IO.INPUT_PATTERN, pattern);
            TextInputFormat.setInputPathFilter(job, RegexPathFilter.class);
        }

        if (!StringUtils.isEmpty(selectorString)) {
            //we do the filtering on our own
            for (FileStatus file : selectFiles(job, basePath, selectorString)) {
                TextInputFormat.addInputPath(job, file.getPath());
            }
        } else {
            TextInputFormat.setInputPaths(job, inputPath);
        }

    }

    private List<String> getFileInputs(String basePath,final Job job) throws IOException {
        final String inputPathFile = job.getConfiguration().get("mapreduce.input.path.file");
        final List<String> paths = Lists.newArrayList(basePath);
        if (!StringUtils.isEmpty(inputPathFile)) {
            Files.readLines(new File(inputPathFile), Charsets.UTF_8, new LineProcessor<Object>() {
                @Override
                public boolean processLine(String s) throws IOException {
                    paths.add(s);
                    return true;
                }

                @Override
                public Object getResult() {
                    return paths;
                }
            });
        }

        return paths;
    }

    private List<FileStatus> selectFiles(Job job, String basePath, String selectorString) throws IOException {
        Selector selector = Selectors.get(selectorString);
        String[] parts = selectorString.split(":");
        int n = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
        List<FileStatus> files = selector.select(job, new Path(basePath), n);
        return files;
    }

    public String getName() {
        return "text";
    }


}
