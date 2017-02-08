package com.rackspace.volga.etl.mapreduce.args;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;


public interface ArgsParser {
    CommandLine parse(String[] args, Option... extras) throws ParseException;
}
