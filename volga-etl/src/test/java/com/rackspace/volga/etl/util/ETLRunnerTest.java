package com.rackspace.volga.etl.util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.jayway.jsonpath.JsonPath;
import com.rackspace.volga.etl.common.transform.DozerTransformer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * User: alex.silva
 * Date: 4/10/15
 * Time: 1:32 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class ETLRunnerTest {
    private String[] args;

    ETLRunner runner;

    File output;

    @Before
    public void setUp() throws IOException {
        runner = new ETLRunner();
        output = Files.createTempDir();
        args = new String[]{"-o", output.getAbsolutePath(), "-i",
                new ClassPathResource("stacktach/stacktach-single.json").getFile().getAbsolutePath(),
                "-volga.etl.output.mapping.id", "fact_st_events", "-volga.etl.dto.class",
                "com.jayway.jsonpath.JsonPath",
                "-volga.etl.input.transformer.class", DozerTransformer.class.getCanonicalName()};
    }

    @Test
    public void whenNotProvidingAnInputRunner() throws Exception {
        String[] args = new String[]{"-o", output.getAbsolutePath(), "-i",
                new ClassPathResource("stacktach/stacktach-single.json").getFile().getAbsolutePath(),
                "-volga.etl.output.mapping.id", "fact_st_events", "-volga.etl.dto.class",
                "com.jayway.jsonpath.JsonPath"};
        runner.setUp(ETLRunner.buildCommandLine(args));
        Assert.assertEquals(runner.inputTransformer.getClass(), DozerTransformer.class);
    }


    @Test
    public void whenSettingUp() throws Exception {
        runner.setUp(ETLRunner.buildCommandLine(args));
        Assert.assertEquals(runner.inputTransformer.getClass(), DozerTransformer.class);
        Assert.assertEquals(null, runner.inputMappingId);
        Assert.assertEquals("fact_st_events", runner.outputMappingId);
        Assert.assertEquals(JsonPath.class, runner.dtoClass);
        Assert.assertEquals(output.getAbsoluteFile(), runner.outputPath);
    }

    @Test
    public void whenMapping() throws Exception {
        runner.setUp(ETLRunner.buildCommandLine(args));
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());
        runner.map(json);
        String mappedRow = Files.readFirstLine(new File(output, "event_definitions"), Charsets.UTF_8);
        Assert.assertEquals("compute.instance.update,deleting,active,6093664,512,20,1," +
                "c0d405953f374ee1b72e5a012fa21143,512MB Standard Instance,0,d4bd46e8-0ace-49c8-b9b9-a66491b2254a," +
                "512MB Standard Instance,,None.nova-api03-r2961.global.preprod-ord.ohthree.com,2015-02-19 18:32:47,," +
                "5.8,,testservercc754701,,req-c6cebdfc-67a9-43e4-a4aa-45101867cac6,0,x64,2," +
                "None.nova-api03-r2961.global.preprod-ord.ohthree.com,20,org.centos", mappedRow);
    }

    @Test
    public void whenRunning() throws Exception {
        runner.run(args);
        String mappedRow = Files.readFirstLine(new File(output, "event_definitions"), Charsets.UTF_8);
        Assert.assertEquals("compute.instance.update,deleting,active,6093664,512,20,1," +
                "c0d405953f374ee1b72e5a012fa21143,512MB Standard Instance,0,d4bd46e8-0ace-49c8-b9b9-a66491b2254a," +
                "512MB Standard Instance,,None.nova-api03-r2961.global.preprod-ord.ohthree.com,2015-02-19 18:32:47,," +
                "5.8,,testservercc754701,,req-c6cebdfc-67a9-43e4-a4aa-45101867cac6,0,x64,2," +
                "None.nova-api03-r2961.global.preprod-ord.ohthree.com,20,org.centos", mappedRow);
    }

    @Test
    public void whenComputingInputs() throws Exception {
        File input = Files.createTempDir();
        List<File> inputs = ETLRunner.computeInputs(input.getPath());
        Assert.assertEquals(0, inputs.size());
        File inputF = new File(input, "test");
        inputF.createNewFile();
        inputs = ETLRunner.computeInputs(input.getPath());
        Assert.assertEquals(1, inputs.size());
        Assert.assertEquals(inputF.getAbsolutePath(), inputs.get(0).getAbsolutePath());
        File inputHidden = new File(input, ".hidden");
        File inputDir = new File(input, "dir");
        inputHidden.createNewFile();
        inputDir.mkdir();
        inputs = ETLRunner.computeInputs(input.getPath());
        Assert.assertEquals(isWindows() ? 2 : 1, inputs.size());

    }

    private boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }


}
