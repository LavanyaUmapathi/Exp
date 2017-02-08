package com.rackspace.volga.etl.mapreduce.lib.selectors;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * User: alex.silva
 * Date: 6/4/14
 * Time: 10:22 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class DescendingDateSelector implements Selector {
    @Override
    public List<FileStatus> select(Job job, Path path, int n) throws IOException {
        FileSystem fSystem = FileSystem.get(job.getConfiguration());
        FileStatus fs = fSystem.getFileStatus(path);
        if (!fs.isDirectory()) {
            throw new IllegalArgumentException(path + " must be a directory to use a date selector.");
        }
        FileStatus[] files = fSystem.listStatus(path);

        Arrays.sort(files, new Comparator<FileStatus>() {
            public int compare(final FileStatus o1, final FileStatus o2) {
                return new Long(o2.getModificationTime()).compareTo(o1.getModificationTime());
            }
        });

        List retVal = Lists.newArrayList();

        for (int i = 0; i < n && i < files.length; i++) {
            retVal.add(files[i]);
        }

        return retVal;

    }
}
