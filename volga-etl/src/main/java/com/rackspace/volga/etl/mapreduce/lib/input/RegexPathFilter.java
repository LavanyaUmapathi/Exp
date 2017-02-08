package com.rackspace.volga.etl.mapreduce.lib.input;

import com.rackspace.volga.etl.mapreduce.lib.io.IO;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexPathFilter implements Configurable, PathFilter {

    private Pattern pattern;

    private Configuration conf;


    @Override
    public boolean accept(Path path) {
        try {
            if (FileSystem.get(conf).getFileStatus(path).isDirectory()) {
                return true;
            } else {
                Matcher m = pattern.matcher(path.toString());
                final boolean isMatch = m.matches();
                System.out.println("Path : " + path.toString() + (isMatch ? " matches " : " does not match ")
                        + " input pattern '" + conf.get(IO.INPUT_PATTERN) + "'");
                return isMatch;
            }
        } catch (IOException e) {
            System.err.println("Unable to accept path due to error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        pattern = Pattern.compile(conf.get(IO.INPUT_PATTERN, StringUtils.EMPTY));

    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
