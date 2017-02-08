package com.rackspace.volga.etl.stacktach.dozer;

import com.google.common.io.Files;
import com.jayway.jsonpath.DocumentContext;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.JSONPathConverter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/8/15
 * Time: 7:55 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class JSONPathToMappedRowsConverterTest {
    private JSONPathToMappedRowsConverter c;

    private DocumentContext dc;

    @Before
    public void setUp() throws Exception {
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single.json").getFile(),
                Charset.defaultCharset());

        dc = new JSONPathConverter().convertFrom(json);

        c = new JSONPathToMappedRowsConverter();
    }

    @Test(expected = IllegalStateException.class)
    public void whenNoParameterSet() {
        c.convert(null, dc, MappedRows.class, DocumentContext.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenInvalidPath() {
        c.setParameter("fake");
        c.convert(null, dc, MappedRows.class, DocumentContext.class);
    }

    @Test
    public void whenConverting() {
        c.setParameter("classpath:mappings/stacktach/event_definitions.yaml");
        MappedRows rows = (MappedRows) c.convert(new MappedRows(), dc, MappedRows.class, DocumentContext.class);
        Assert.assertEquals(1, rows.getRows().size());
        
        Assert.assertEquals("compute.instance.update,deleting,active,6093664,512,20,1," +
                        "c0d405953f374ee1b72e5a012fa21143,512MB Standard Instance,0," +
                        "d4bd46e8-0ace-49c8-b9b9-a66491b2254a,512MB Standard Instance,," +
                        "None.nova-api03-r2961.global.preprod-ord.ohthree.com,2015-02-19 18:32:47,,5.8,," +
                        "testservercc754701,,req-c6cebdfc-67a9-43e4-a4aa-45101867cac6,0,x64,2," +
                        "None.nova-api03-r2961.global.preprod-ord.ohthree.com,20,org.centos",
                ((List) rows.getRows().get("event_definitions")).get(0));
    }

    @Test
    public void whenConvertingNonMatchingEvent() throws Exception {
        c.setParameter("classpath:mappings/stacktach/event_definitions.yaml");
        String json = Files.readFirstLine(new ClassPathResource("stacktach/stacktach-single-scheduler.json").getFile(),
                Charset.defaultCharset());
        dc = new JSONPathConverter().convertFrom(json);
        MappedRows rows = (MappedRows) c.convert(new MappedRows(), dc, MappedRows.class, DocumentContext.class);
        Assert.assertEquals(0, rows.getRows().size());
    }
}
