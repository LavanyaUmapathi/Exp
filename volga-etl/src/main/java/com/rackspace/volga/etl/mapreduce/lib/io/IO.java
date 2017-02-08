package com.rackspace.volga.etl.mapreduce.lib.io;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.io.IOException;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/2/14
 * Time: 4:51 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class IO {

    private static Map<String, IOConfig> outputs = Maps.newHashMap();

    private static Map<String, IOConfig> inputs = Maps.newHashMap();

    public static final String INPUT_PATTERN = "input.pattern";


    static {
        try {
            outputs = loadIOConfigsFromClasspath("com.rackspace.volga.etl.mapreduce.lib.output");
            inputs = loadIOConfigsFromClasspath("com.rackspace.volga.etl.mapreduce.lib.input");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setOutput(String type, Job job) throws IOException {
        IOConfig cfg = outputs.get(type);
        if (cfg == null) {
            throw new IllegalArgumentException(type + " is not a known output type.");
        }

        cfg.configure(job);
    }

    public static void setInput(String type, Job job) throws IOException {
        IOConfig cfg = inputs.get(type);
        if (cfg == null) {
            throw new IllegalArgumentException(type + " is not a known input type.");
        }
        cfg.configure(job);
    }

    private static Map<String, IOConfig> loadIOConfigsFromClasspath(String pkgName) throws ClassNotFoundException {
        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AssignableTypeFilter(IOConfig.class));
        Map<String, IOConfig> map = Maps.newHashMap();
        for (BeanDefinition bd : scanner.
                findCandidateComponents(pkgName)) {
            IOConfig o = BeanUtils.instantiateClass(Class.forName(bd.getBeanClassName()), IOConfig.class);
            map.put(o.getName(), o);
        }

        return map;
    }
}
