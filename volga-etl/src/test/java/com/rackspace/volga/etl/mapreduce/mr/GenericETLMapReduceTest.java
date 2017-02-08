package com.rackspace.volga.etl.mapreduce.mr;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.ETLConstants;
import com.rackspace.volga.etl.maas.dto.json.Configuration;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import com.rackspace.volga.etl.maas.dto.json.Notification;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 7/24/14
 * Time: 4:29 PM
 * Copyright Rackspace Hosting, Inc.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({GenericETLReducer.class})
@PowerMockIgnore("javax.management.*")
public class GenericETLMapReduceTest {
    private String mjson;

    private String njson;

    private String cjson;

    MapDriver<LongWritable, Text, Text, Text> mapDriver;

    ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;

    MapReduceDriver<LongWritable, Text, Text, Text, NullWritable, Text> mapReduceDriver;

    private static final String ALARM_LINE = "alWusVry77," +
            "body match - string not found," +
            "enPUVwV0IL,ac5rNRE5nk,chUWmQjirR,false,npTechnicalContactsEmail,1364684157128,1364684157128," +
            "template_name=body match - string not found";

    private static final String CHECK_LINE = "chUWmQjirR,enPUVwV0IL," +
            "ac5rNRE5nk," +
            "www.everloop.com,remote.http,30,60,,www.everloop.com,ipv4,false,1364684154472,1383153071044," +
            "url=http://www.everloop.com|body=kids privacy " +
            "policy|method=get|auth_user=|auth_password=|follow_redirects=true|payload=|include_body=false," +
            "mzdfw|mzlon|mzord,,enPUVwV0IL";

    private static final String ENTITY_LINE = "enPUVwV0IL,," +
            "everloop-load-balancer,false," +
            "https://servers.api.rackspacecloud.com/v1.0/487439/servers/202936,,1344564747355,1344564747355," +
            "public0_v4=173.203.51.14|private0_v4=10.179.65.243,";

    private static final String NOTIFICATION_LINE = "ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL," +
            "6be23b20-bd18-11e3-9fae-eb57b6f62ca9,487439,enPUVwV0IL,chUWmQjirR,remote.http,alWusVry77," +
            "www.everloop.com,1396739833810,4,5,2014,http response did not contain the correct content.,critical," +
            "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
            "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
            "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP response did" +
            " not contain the correct content.\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP response contains" +
            " the correct content\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP response did not contain" +
            " the correct content.\u00041396739787101\u0004";

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    @Before
    public void setUp() throws IOException {
        mjson = Files.readFirstLine(new ClassPathResource("maas/metrics.json").getFile(), Charset.defaultCharset());
        njson = Files.readFirstLine(new ClassPathResource("maas/notifications.json").getFile(), Charset.defaultCharset());
        cjson = Files.readFirstLine(new ClassPathResource("maas/configurations.json").getFile(), Charset.defaultCharset());
        GenericETLMapper mapper = new GenericETLMapper();
        GenericETLReducer reducer = new GenericETLReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperWithMetrics() throws IOException {
        mapDriver.getConfiguration().set(ETLConstants.DTO_CLASS_KEY, Metric.class.getName());
        mapDriver.withInput(new LongWritable(), new Text(mjson));
        mapDriver.withOutput(new Text("1395941810615-metric"), new Text(",ac4ubIxzmW,898450,enC6anIcNY,chOdIdiZu0,,," +
                "agent.load_average,,,true,1395941810615,3,27,2014,15m=110\u00040.05\u0004unknown\u0004|5m=110\u00040" +
                ".01\u0004unknown\u0004|1m=110\u00040\u0004unknown\u0004"));
        mapDriver.runTest();
        testValidCounters(new DateTime(1395941810615l), 1);
    }

    @Test
    public void testMapperWithNotifications() throws IOException {
        mapDriver.getConfiguration().set(ETLConstants.DTO_CLASS_KEY, Notification.class.getName());
        mapDriver.withInput(new LongWritable(), new Text(njson));
        mapDriver.addOutput(new Text("1396739833810-notification"), new Text(NOTIFICATION_LINE));
        mapDriver.addOutput(new Text("1396739833810-entity"), new Text(ENTITY_LINE));
        mapDriver.addOutput(new Text("1396739833810-check"), new Text(CHECK_LINE));
        mapDriver.addOutput(new Text("1396739833810-alarm"), new Text(ALARM_LINE));
        mapDriver.runTest();
        testValidCounters(new DateTime(1396739833810l), 1);
    }

    @Test
    public void testMapperWithConfigurations() throws IOException {
        mapDriver.getConfiguration().set(ETLConstants.DTO_CLASS_KEY, Configuration.class.getName());
        mapDriver.withInput(new LongWritable(), new Text(cjson));
        String configuration = "ac006xgyTj,824378,30,5184000,GOOD," +
                "false,cgC,stable,stable,,enRayzGfb6," +
                "global=50000|test_check=500|test_alarm=500|test_notification=200|traceroute=300," +
                "full=48|5m=168|20m=360|60m=720|240m=1440|1440m=8760";
        String entity = "enRayzGfb6,ac006xgyTj,zapmove,false," +
                "https://ord.servers.api.rackspacecloud.com/824378/servers/f3c9ef75-4899-4e77-88c1-ce2261b0b15a,," +
                "1366957208601,1366957208601," +
                "access_ip0_v6=2001:4801:7819:0074:ee84:d5c2:ff10:9c20|public0_v6=2001:4801:7819:0074:ee84:d5c2:ff10" +
                ":9c20|access_ip1_v4=162.209.1.70|private0_v4=10.178.19.7|public1_v4=162.209.1.70,";
        mapDriver.addOutput(new Text("1334245623-configuration"), new Text(configuration));
        mapDriver.addOutput(new Text("1334245623-entity"), new Text(entity));

        List<Pair<Text, Text>> results = mapDriver.run();
        assertEquals(2, results.size());
        assertEquals(configuration, results.get(0).getSecond().toString());
        assertEquals(entity, results.get(1).getSecond().toString());
    }

    private void testValidCounters(DateTime dt, int val) {
        assertEquals("Expected 1 valid counter increment", val, mapDriver.getCounters()
                .findCounter(ETLDriverUtils.Counters.VALID).getValue());
        assertEquals("Expected 1 counter by date group increment", val, mapDriver.getCounters()
                .findCounter(ETLDriverUtils.VALID_COUNTER_GROUP, dt.toString(formatter)).getValue());
    }

    @Test
    public void testMapperWithBadJson() throws IOException {
        mapDriver.getConfiguration().set(ETLConstants.DTO_CLASS_KEY, Metric.class.getName());
        mapDriver.withInput(new LongWritable(), new Text(mjson.substring(3)));
        try {
            mapDriver.runTest();
        } catch (Exception e) {
            //expected
        }
        DateTime dt = new DateTime(1395941810615l);
        testValidCounters(dt, 0);
        assertEquals("Expected 1 error counter increment", 1, mapDriver.getCounters()
                .findCounter(ETLDriverUtils.Counters.ERRORS).getValue());

        //0 because can't parse the timestamp
        assertEquals("Expected 1 counter by date group increment", 0, mapDriver.getCounters()
                .findCounter(ETLDriverUtils.INVALID_COUNTER_GROUP, dt.toString(formatter)).getValue());
    }

    @Test
    public void testReducerWithNotification() throws IOException {
    	reduceDriver
    			.withInput(new Text("1396739833810-alarm"), Lists.newArrayList(new Text(ALARM_LINE)))
    			.withInput(new Text("1396739833810-check"), Lists.newArrayList(new Text(CHECK_LINE)))
    			.withInput(new Text("1396739833810-entity"), Lists.newArrayList(new Text(ENTITY_LINE)))
    			.withInput(new Text("1396739833810-notification"), Lists.newArrayList(new Text(NOTIFICATION_LINE)))
                .withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text(ALARM_LINE)), "alarm")
                .withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text(CHECK_LINE)), "check")
                .withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text(ENTITY_LINE)), "entity")
                .withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text(NOTIFICATION_LINE)),
                        "notification")
                .runTest();
    }

}
