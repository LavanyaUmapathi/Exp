package com.rackspace.volga.etl.common.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.jayway.jsonpath.DocumentContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * User: alex.silva
 * Date: 4/2/15
 * Time: 12:39 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class JSONPathConverterTest {
    private JSONPathConverter t;

    @Before
    public void setUp() throws IOException {
        t = new JSONPathConverter();
    }

    @Test
    public void whenLoadingJSON() throws IOException {
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());

        DocumentContext obj = t.convertFrom(json);
        Assert.assertEquals("c0d405953f374ee1b72e5a012fa21143", obj.read("_context_user_id"));
        Assert.assertEquals(20, obj.read("payload.root_gb"));
    }

    @Test
    public void whenWritingJSON() throws IOException {
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());

        DocumentContext obj = t.convertFrom(json);
        String njson = t.convertTo(obj);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree1 = mapper.readTree(json);
        JsonNode tree2 = mapper.readTree(njson);
        Assert.assertEquals(tree1, tree2);
    }
}
