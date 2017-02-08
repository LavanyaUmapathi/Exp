package com.rackspace.volga.etl.stacktach;

import com.google.common.io.Files;
import com.jayway.jsonpath.DocumentContext;
import com.rackspace.volga.etl.common.transform.JSONPathConverter;
import com.rackspace.volga.etl.stacktach.yaml.StackDistiller;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/8/15
 * Time: 4:24 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class StackDistillerTest {
    private StackDistiller distiller;

    private DocumentContext dc;

    @Before
    public void setUp() throws Exception {
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());

        dc = new JSONPathConverter().convertFrom(json);
        distiller = new StackDistiller("classpath:mappings/stacktach/event_definitions.yaml");
    }

    @Test(expected = FileNotFoundException.class)
    public void whenNoValidYamlFile() throws Exception {
        new StackDistiller("classpath:saywhat");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNullYamlFile() throws Exception {
        new StackDistiller(null);
    }

    @Test
    public void whenDistilling(){
        Map<String,String> dict = distiller.distill(dc);
        Assert.assertEquals("compute.instance.update", dict.get("event_type"));
        Assert.assertEquals("deleting", dict.get("state_description"));
        Assert.assertEquals("active", dict.get("state"));
        Assert.assertEquals("6093664", dict.get("tenant_id"));
        Assert.assertEquals("512", dict.get("memory_mb"));
        Assert.assertEquals("20", dict.get("root_gb"));
        Assert.assertEquals("1", dict.get("vcpus"));
        Assert.assertEquals("c0d405953f374ee1b72e5a012fa21143", dict.get("user_id"));
        Assert.assertEquals("512MB Standard Instance", dict.get("instance_flavor"));
        Assert.assertEquals("0", dict.get("rax_options"));
        Assert.assertEquals("d4bd46e8-0ace-49c8-b9b9-a66491b2254a", dict.get("instance_id"));
        Assert.assertEquals("512MB Standard Instance", dict.get("instance_type"));
        Assert.assertEquals("", dict.get("bandwidth_in"));
        Assert.assertEquals("None.nova-api03-r2961.global.preprod-ord.ohthree.com", dict.get("host"));
        Assert.assertEquals("2015-02-19 18:32:47", dict.get("launched_at"));
        Assert.assertEquals("", dict.get("deleted_at"));
        Assert.assertEquals("5.8", dict.get("os_version"));
        Assert.assertEquals("", dict.get("message"));
        Assert.assertEquals("testservercc754701", dict.get("display_name"));
        Assert.assertEquals("", dict.get("bandwidth_out"));
        Assert.assertEquals("req-c6cebdfc-67a9-43e4-a4aa-45101867cac6", dict.get("request_id"));
        Assert.assertEquals("0", dict.get("ephemeral_gb"));
        Assert.assertEquals("x64", dict.get("os_architecture"));
        Assert.assertEquals("2", dict.get("instance_flavor_id"));
        Assert.assertEquals("None.nova-api03-r2961.global.preprod-ord.ohthree.com", dict.get("service"));
        Assert.assertEquals("20", dict.get("disk_gb"));
        Assert.assertEquals("org.centos", dict.get("os_distro"));
    }
}
