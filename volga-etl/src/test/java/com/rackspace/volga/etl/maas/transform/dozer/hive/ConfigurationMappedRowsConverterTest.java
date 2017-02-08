package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.maas.dto.json.Configuration;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * User: alex.silva
 * Date: 7/24/14
 * Time: 11:58 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class ConfigurationMappedRowsConverterTest {
    private ConfigurationMappedRowsConverter cvt = new ConfigurationMappedRowsConverter();

    @Test
    public void whenPassingExistingMappedRow() throws IOException {
        String json = Files.readFirstLine(new ClassPathResource("maas/configurations.json").getFile(),
                Charset.defaultCharset());
        Configuration m = new JacksonConverter<Configuration>(Configuration.class).convertFrom(json);
        MappedRows rows = new MappedRows();
        MappedRows nrows = cvt.convertFrom(m, rows);
        assertSame(nrows, rows);
    }

    @Test
    public void testConvert() throws IOException {
        String json = Files.readFirstLine(new ClassPathResource("maas/configurations.json").getFile(),
                Charset.defaultCharset());
        Configuration c = new JacksonConverter<Configuration>(Configuration.class).convertFrom(json);
        MappedRows rows = cvt.convertFrom(c);
        assertEquals(rows.getRows().size(), 2);
        assertEquals(((List) rows.getRows().get("entity")).get(0), "enRayzGfb6,ac006xgyTj,zapmove,false," +
                "https://ord.servers.api.rackspacecloud.com/824378/servers/f3c9ef75-4899-4e77-88c1-ce2261b0b15a,," +
                "1366957208601,1366957208601," +
                "access_ip0_v6=2001:4801:7819:0074:ee84:d5c2:ff10:9c20|public0_v6=2001:4801:7819:0074:ee84:d5c2:ff10" +
                ":9c20|access_ip1_v4=162.209.1.70|private0_v4=10.178.19.7|public1_v4=162.209.1.70,");
        assertEquals(((List) rows.getRows().get("configuration")).get(0), "ac006xgyTj,824378,30,5184000,GOOD,false," +
                "cgC,stable,stable,,enRayzGfb6," +
                "global=50000|test_check=500|test_alarm=500|test_notification=200|traceroute=300," +
                "full=48|5m=168|20m=360|60m=720|240m=1440|1440m=8760");
    }

    @Test
    public void testLongerConvert() throws IOException {
        List<String> json = Files.readLines(new ClassPathResource("maas/configurations.json").getFile(),
                Charset.defaultCharset());
        Configuration c = new JacksonConverter<Configuration>(Configuration.class).convertFrom(json.get(1));
        MappedRows rows = cvt.convertFrom(c);
        assertEquals(rows.getRows().size(), 5);
        assertEquals(((List) rows.getRows().get("entity")).get(0), "enzUPArNob,ac003A6Ctq,vw-web1,false," +
                "https://dfw.servers.api.rackspacecloud.com/834586/servers/6b6c8c86-642a-4647-b6a1-64ec0fac9cbe," +
                "hazhuellaconvocho.com,1372786897430,1372973771312," +
                "access_ip0_v6=2001:4800:780e:0510:ebb0:d468:ff05:2e74|access_ip1_v4=50.56.172.84|public0_v4=50.56" +
                ".172.84|public1_v6=2001:4800:780e:0510:ebb0:d468:ff05:2e74|private0_v4=10.180.38.166,");
        assertEquals(((List) rows.getRows().get("configuration")).get(0), "ac003A6Ctq,834586,30,5184000,GOOD,false," +
                "cgD,stable,stable,huellavocho,enzUPArNob," +
                "global=50000|test_check=500|test_alarm=500|test_notification=200|traceroute=300," +
                "full=48|5m=168|20m=360|60m=720|240m=1440|1440m=8760");
        assertEquals(((List) rows.getRows().get("alarm")).get(0), "alnWrhyKOv,connection time,enzUPArNob,ac003A6Ctq," +
                "ch9VgSAhux,false,npTechnicalContactsEmail,1374256628469,1374256628469," +
                "template_name=remote.tcp_connection_time");
        assertEquals(((List) rows.getRows().get("check")).get(0), "ch9VgSAhux,enzUPArNob,ac003A6Ctq,checktcp," +
                "remote.tcp,30,60,public0_v4,,,false,1374256619673,1405023763402," +
                "port=80|banner_match=|send_body=|body_match=|ssl=false,mzdfw|mzlon|mzord,");
        assertEquals(((List) rows.getRows().get("contact")).get(0), "huellavocho,luis,contreras,luis@inventmx.com," +
                "billing,https://customer.api.rackspacecloud" +
                ".com/v1/customer_accounts/cloud/834586/contacts/rpn-949-758-465,1393051747626");
        assertEquals(((List) rows.getRows().get("contact")).get(1), "huellavocho,luis,contreras,luis@inventmx.com," +
                "primary,https://customer.api.rackspacecloud" +
                ".com/v1/customer_accounts/cloud/834586/contacts/rpn-856-792-691,1393051747626");
    }
}
