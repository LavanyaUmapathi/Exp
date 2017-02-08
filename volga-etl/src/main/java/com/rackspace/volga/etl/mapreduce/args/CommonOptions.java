package com.rackspace.volga.etl.mapreduce.args;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;

import java.util.Collection;

public class CommonOptions {
    private static final Option ZOOKEEPER_QUORUM = new Option("zq", "hbase.zookeeper.quorum", true,
            "HBase Master Zookeeper Instance");

    private static final Option REDUCER_CLASS = new Option("rd", "mapreduce.reducer.class", true,
            "Reducer class name.");

    private static final Option START_DATE = new Option("sd", "start.date", true,
            "The start date in yyyy-MM-dd format. Defaults to today.");

    private static final Option END_DATE = new Option("ed", "end.date", true, "The end date in yyyy-MM-dd format. " +
            "Defaults to today.");

    private static final Option INPUT_PATH = new Option("i", "mapreduce.input.path", true, "The input path URL.");

    private static final Option INPUT_PATH_FILE = new Option("if", "mapreduce.input.path.file", true,
            "A file containing a list of input paths to be added as inputs to the job.  One path per line.");

    private static final Option OUTPUT_PATH = new Option("o", "mapreduce.output.dir", true, "The output dir URL.");

    private static final Option OUTPUT_FORMAT = new Option("of", "mapreduce.output.format", true,
            "The output format (csv,hbase,text,etc.).");

    private static final Option INPUT_FORMAT = new Option("of", "mapreduce.input.format", true,
            "The input format (csv,json,text,etc.).");

    private static final Option DEBUG = new Option("d", "debug", false, "switch on DEBUG log level.");

    private static final Option TEXTOUTPUTFORMAT_SEPARATOR = new Option("sp", "output.separator", true,
            "The TextOutputFormat separator.");

    private static final Option INPUT_REGEX_PATTERN = new Option("ip", "mapreduce.input.pattern", true,
            "The input file(s) regex pattern.");

    private static final Option SELECTOR = new Option("s", "mapreduce.input.selector", true,
            "The input selector name and args, in <name>:<arg> format.");

    private static final Option INPUT_MAPPING_ID = new Option("im", "volga.etl.input.mapping.id", true,
            "The mapping id to use in the transformation (optional.)");

    private static final Option OUTPUT_MAPPING_ID = new Option("om", "volga.etl.output.mapping.id", true,
            "The mapping id to use in the transformation (optional.)");

    static {
        ZOOKEEPER_QUORUM.setArgName("hbase-zookeeper");
        START_DATE.setArgName("start.date");
        END_DATE.setArgName("end.date");
        INPUT_PATH.setArgName("input.path");
        INPUT_PATH_FILE.setArgName("input.path.file");
        REDUCER_CLASS.setArgName("mapreduce.reducer.class");
        REDUCER_CLASS.setRequired(false);
        INPUT_PATH.setRequired(true);
        INPUT_PATH.setValueSeparator(',');
        INPUT_PATH.setRequired(false);
        OUTPUT_PATH.setArgName("output");
        OUTPUT_FORMAT.setArgName("output.format");
        INPUT_FORMAT.setArgName("input.format");
        OUTPUT_PATH.setRequired(true);
        TEXTOUTPUTFORMAT_SEPARATOR.setArgName("output.separator");
        INPUT_REGEX_PATTERN.setRequired(false);
        SELECTOR.setRequired(false);
        INPUT_MAPPING_ID.setRequired(false);
        INPUT_MAPPING_ID.setArgName("input.mapping.id");
        OUTPUT_MAPPING_ID.setRequired(false);
        OUTPUT_MAPPING_ID.setArgName("output.mapping.id");
    }

    public static Collection<Option> getOptions() {
        return Lists.newArrayList(ZOOKEEPER_QUORUM, START_DATE,
                END_DATE, INPUT_PATH, INPUT_PATH_FILE, OUTPUT_PATH, DEBUG, TEXTOUTPUTFORMAT_SEPARATOR, OUTPUT_FORMAT,
                REDUCER_CLASS,
                INPUT_REGEX_PATTERN, SELECTOR, OUTPUT_MAPPING_ID, INPUT_MAPPING_ID);
    }


}
