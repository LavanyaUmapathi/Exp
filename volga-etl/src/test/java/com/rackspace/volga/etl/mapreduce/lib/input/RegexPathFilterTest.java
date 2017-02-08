package com.rackspace.volga.etl.mapreduce.lib.input;

import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 3:25 PM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.logging.log4j.core.jmx.*")
@PrepareForTest({FileSystem.class, Throwable.class})
public class RegexPathFilterTest {

    private RegexPathFilter filter;

    private Configuration conf;

    FileSystem fs;

    @Before
    public void setup() throws Exception {
        filter = new RegexPathFilter();
        conf = new Configuration();
        filter.setConf(conf);
        PowerMockito.mockStatic(FileSystem.class);
        fs = PowerMockito.mock(FileSystem.class);
        PowerMockito.when(FileSystem.class, "get", conf).thenReturn(fs);

    }

    @Test
    public void whenTestingConfigurable() throws Exception {
        assertEquals(conf, filter.getConf());
    }

    @Test
    public void whenFilteringDirectory() throws Exception {
        Path path = PowerMockito.mock(Path.class);
        PowerMockito.when(fs.getFileStatus(path)).thenReturn(new FileStatus(10l, true, 1, 1l, 1l, path));
        PowerMockito.when(path.getFileSystem(conf)).thenReturn(fs);
        assertTrue(filter.accept(path));
    }

    @Test
    public void whenFilteringFileWithNoInputPatternPresent() throws Exception {
        Path path = new Path("/what/a/fake/path");
        PowerMockito.when(fs.getFileStatus(path)).thenReturn(new FileStatus(10l, false, 1, 1l, 1l, path));
        PowerMockito.when(path.getFileSystem(conf)).thenReturn(fs);
        assertFalse(filter.accept(path));
    }

    @Test
    public void whenExceptionIsThrown() throws Exception {
        Path path = new Path("/what/a/fake/path");
        PowerMockito.when(FileSystem.class, "get", conf).thenThrow(new IOException("Invalid path"));
        assertFalse(filter.accept(path));
    }

    @Test
    public void whenFilteringFileMatching() throws Exception {
        Path path = new Path("fake-path");
        conf.set(IO.INPUT_PATTERN, ".*fake.*");
        filter.setConf(conf);
        PowerMockito.when(fs.getFileStatus(path)).thenReturn(new FileStatus(10l, false, 1, 1l, 1l, path));
        PowerMockito.when(path.getFileSystem(conf)).thenReturn(fs);
        assertTrue(filter.accept(path));

    }

}
