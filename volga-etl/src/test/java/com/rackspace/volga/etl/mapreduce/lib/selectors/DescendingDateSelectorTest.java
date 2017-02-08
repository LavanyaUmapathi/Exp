package com.rackspace.volga.etl.mapreduce.lib.selectors;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

@PrepareForTest(FileSystem.class)
@PowerMockIgnore("org.apache.logging.log4j.core.jmx.*")
@RunWith(PowerMockRunner.class)
public class DescendingDateSelectorTest {

    @Test(expected = IllegalArgumentException.class)
    public void whenSelectingWithoutAdirectory() throws IOException {
        DescendingDateSelector ds = new DescendingDateSelector();
        PowerMockito.mockStatic(FileSystem.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fs);

        FileStatus status = new FileStatus(10, false, 10, 10l, 10l, null);

        Mockito.when(fs.getFileStatus(Mockito.any(Path.class))).thenReturn(status);
        ds.select(Mockito.mock(Job.class), Mockito.mock(Path.class), 0);
    }

    @Test
    public void whenSelecting() throws IOException {
        DescendingDateSelector ds = new DescendingDateSelector();
        PowerMockito.mockStatic(FileSystem.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(FileSystem.get(Mockito.any(Configuration.class))).thenReturn(fs);
        FileStatus s1 = new FileStatus(10, true, 10, 10l, 1l, null);
        FileStatus s2 = new FileStatus(10, true, 10, 10l, 2l, null);
        FileStatus s3 = new FileStatus(10, true, 10, 10l, 3l, null);
        FileStatus s4 = new FileStatus(10, true, 10, 10l, 4l, null);
        Mockito.when(fs.getFileStatus(Mockito.any(Path.class))).thenReturn(s1);
        Mockito.when(fs.listStatus(Mockito.any(Path.class))).thenReturn(new FileStatus[]{s1, s2, s3, s4});
        List<FileStatus> result = ds.select(Mockito.mock(Job.class), Mockito.mock(Path.class), 3);
        assertEquals(result, Lists.newArrayList(s4, s3, s2));
    }
}
