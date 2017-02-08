package com.rackspace.volga.etl.mapreduce.args;

import org.apache.commons.cli.*;


public class CommandLineBuilder {

    CommandLineParser parser = new PosixParser();

    public CommandLine build(Options options, String[] args) {
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("[[Driver]] ", options, true);
            throw new IllegalArgumentException(e.getMessage());
        }
        return cmd;
    }

    public CommandLine buildCommonOptions(String[] args, Option... extras) {
        Options options = new Options();
        for (Option option : CommonOptions.getOptions()) {
            options.addOption(option);
        }

        if (extras != null) {
            for (Option option : extras) {
                options.addOption(option);
            }
        }
        return build(options, args);
    }

}
