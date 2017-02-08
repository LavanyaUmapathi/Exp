package com.rackspace.volga.etl.util;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.DozerTransformer;
import com.rackspace.volga.etl.common.transform.Transformer;
import com.rackspace.volga.etl.mapreduce.args.CommandLineBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.CommandLineRunner;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/10/15
 * Time: 12:21 PM
 * Copyright Rackspace Hosting, Inc.
 * <p/>
 * Runs the ETL in standadlone mode, without going through Hadoop
 */
public class ETLRunner implements CommandLineRunner {

    Transformer inputTransformer;

    Transformer<Object, MappedRows> outputTransformer;

    String inputMappingId;

    String outputMappingId;

    Class<?> dtoClass;

    private Map<String, File> outputs = Maps.newHashMap();

    File outputPath;

    public static void main(String[] args) throws Exception {
        new ETLRunner().run(args);
    }

    public void run(String... args) throws Exception {
        CommandLine cmd = buildCommandLine(args);
        ETLRunner runner = new ETLRunner();
        runner.setUp(cmd);
        for (File file : computeInputs(cmd.getOptionValue("mapreduce.input.path"))) {
            for (String json : Files.readLines(file, Charset.defaultCharset())) {
                runner.map(json);
            }
        }
    }

    public void setUp(CommandLine cmd) throws Exception {
        dtoClass = Class.forName(cmd.getOptionValue(ETLConstants.DTO_CLASS_KEY));
        String inputTransformerArg = cmd.getOptionValue(ETLConstants.INPUT_TRANSFORMER_CLASS_KEY);
        inputTransformer = inputTransformerArg != null ?
                Class.forName(inputTransformerArg).asSubclass(Transformer.class).newInstance() :
                new DozerTransformer();
        outputTransformer = new DozerTransformer();
        inputMappingId = cmd.getOptionValue(ETLConstants.INPUT_MAPPING_ID);
        outputMappingId = cmd.getOptionValue(ETLConstants.OUTPUT_MAPPING_ID);
        outputPath = new File(cmd.getOptionValue("mapreduce.output.dir"));
        Files.createParentDirs(outputPath);
        FileUtils.cleanDirectory(outputPath);
    }

    static List<File> computeInputs(String path) {
        List inputs = Lists.newArrayList();
        File input = new File(path);
        if (!input.isDirectory()) {
            inputs.add(input);
        } else {
            for (File file : Files.fileTreeTraverser().preOrderTraversal(input)) {
                if (!file.isHidden() && !file.isDirectory()) {
                    inputs.add(file);
                }
            }
        }

        return inputs;
    }

    void map(String json) throws Exception {
        Object obj = inputTransformer.transform(json, dtoClass, inputMappingId);
        MappedRows mrows = outputTransformer.transform(obj, MappedRows.class, outputMappingId);

        Map<String, Collection<String>> rowsByKey = mrows.getRows();

        for (Map.Entry<String, Collection<String>> row : rowsByKey.entrySet()) {
            for (String rowValue : row.getValue()) {
                Files.append(rowValue + System.getProperty("line.separator"), getFileForKey(row.getKey()),
                        Charsets.UTF_8);
            }
        }
    }

    private File getFileForKey(String key) {
        File sink = outputs.get(key);
        if (sink == null) {
            sink = new File(outputPath, key);
            outputs.put(key, sink);
        }

        return sink;
    }

    static CommandLine buildCommandLine(String[] args) {
        return new CommandLineBuilder().buildCommonOptions(args,
                new Option("dto", ETLConstants.DTO_CLASS_KEY, true, ""),
                new Option("it", ETLConstants.INPUT_TRANSFORMER_CLASS_KEY, true, ""),
                new Option("imid", ETLConstants.INPUT_MAPPING_ID, true, ""),
                new Option("omid", ETLConstants.OUTPUT_MAPPING_ID, true, ""));
    }
}
