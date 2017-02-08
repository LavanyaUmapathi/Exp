package com.rackspace.volga.etl.mapreduce.args;

import org.apache.commons.cli.*;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: alex.silva
 * Date: 7/24/14
 * Time: 1:53 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class CommandLineBuilderTest {


    private CommandLineBuilder cmd = new CommandLineBuilder();

    private Option o = new Option("t", "tester.option", true, "Desc");

    private Option o1 = new Option("rd", "mapreduce.reducer.class", true, "Reducer class name.");

    @Test
    public void whenBuildingWithOptionsEmptyArgs() {
        Options opt = new Options();
        opt.addOption(o1);
        opt.addOption(o);
        CommandLine line = cmd.build(opt, new String[]{});
        assertEquals(line.getOptionValue("o1"), null);
    }

    @Test
    public void checkErrorMessageIsPrinted() throws IOException, ParseException {
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        PrintStream err = System.err;
        try {
            Options opt = new Options();
            System.setErr(new PrintStream(errContent));
            cmd.parser = PowerMockito.mock(CommandLineParser.class);
            PowerMockito.when(cmd.parser.parse(opt, new String[]{})).thenThrow(IllegalArgumentException.class);
            cmd.build(opt, new String[]{});
        } catch (IllegalArgumentException e) {
            //expected
            errContent.flush();
            assertTrue(errContent.size() > 0);
        } finally {
            System.setErr(err);
        }
    }

    @Test
    public void whenBuildingWithOptions() {
        Options opt = new Options();
        opt.addOption(o1);
        opt.addOption(o);
        CommandLine line = cmd.build(opt, new String[]{"-t", "tvalue", "-rd", "rdvalue"});
        assertEquals(line.getOptionValue("t"), "tvalue");
        assertEquals(line.getOptionValue("rd"), "rdvalue");
    }

    @Test
    public void whenBuildindCommonOptionsNoExtras() {
        CommandLine line = cmd.buildCommonOptions(new String[]{"-i", "ivalue", "-o", "ovalue"});
        assertEquals(line.getOptionValue("i"), "ivalue");
        assertEquals(line.getOptionValue("o"), "ovalue");
    }

    @Test
    public void whenBuildindCommonOptionsExtras() {
        CommandLine line = cmd.buildCommonOptions(new String[]{"-i", "ivalue", "-t", "tvalue", "-o", "ovalue"}, o);
        assertEquals(line.getOptionValue("t"), "tvalue");
        assertEquals(line.getOptionValue("o"), "ovalue");
    }


}
