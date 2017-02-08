package com.rackspace.volga.etl.mapreduce.lib.selectors;

import com.rackspace.volga.etl.utils.CoverageForPrivateConstructor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/25/14
 * Time: 11:06 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class SelectorsTest {

    @Test(expected = IllegalArgumentException.class)
    public void whenGettingUnknownSelector() {
        Selector s = Selectors.get("SelectorTest:5");
        assertEquals(SelectorTest.class, s.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenGettingSelector() {
        Selector s = Selectors.get("SelectorsTest.SelectorTest:5");
        assertEquals(SelectorTest.class, s.getClass());
    }

    private static class SelectorTest implements Selector {

        @Override
        public List<FileStatus> select(Job job, Path path, int n) throws IOException {
            return null;
        }
    }

    @Test
    public void testConstructorPrivate() throws Exception {
        CoverageForPrivateConstructor.giveMeCoverage(Selectors.class);
    }
}
