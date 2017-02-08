package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.io.Files;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.maas.dto.json.Notification;
import org.junit.BeforeClass;
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
public class NotificationMappedRowsConverterTest {
    private NotificationMappedRowsConverter cvt = new NotificationMappedRowsConverter();

    private static List<String> notifications;

    @BeforeClass
    public static void setUp() throws IOException {
        notifications = Files.readLines(new ClassPathResource("maas/notifications.json").getFile(),
                Charset.defaultCharset());
    }

    private String getRow(int i, String type) {
        Notification m = new JacksonConverter<Notification>(Notification.class).convertFrom(notifications.get(i));
        MappedRows rows = cvt.convertFrom(m);
        assertEquals(rows.getRows().size(), 4);
        return (String) ((List) rows.getRows().get(type)).get(0);
    }

    @Test
    public void testLine1() throws IOException {
        String row = getRow(0, "notification");
        assertEquals("ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL," +
                "6be23b20-bd18-11e3-9fae-eb57b6f62ca9,487439,enPUVwV0IL,chUWmQjirR,remote.http,alWusVry77," +
                "www.everloop.com,1396739833810,4,5,2014,http response did not contain the correct content.,critical," +
                "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
                "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
                "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP response did" +
                " not contain the correct content.\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP response contains" +
                " the correct content\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP response did not contain" +
                " the correct content.\u00041396739787101\u0004", row);
        row = getRow(0, "entity");
        assertEquals("enPUVwV0IL,,everloop-load-balancer,false,https://servers.api.rackspacecloud.com/v1" +
                ".0/487439/servers/202936,,1344564747355,1344564747355,public0_v4=173.203.51.14|private0_v4=10.179.65" +
                ".243,", row);
        row = getRow(0, "check");
        assertEquals("chUWmQjirR,enPUVwV0IL,ac5rNRE5nk,www.everloop.com,remote.http,30,60,,www.everloop.com,ipv4," +
                "false,1364684154472,1383153071044,url=http://www.everloop.com|body=kids privacy " +
                "policy|method=get|auth_user=|auth_password=|follow_redirects=true|payload=|include_body=false," +
                "mzdfw|mzlon|mzord,,enPUVwV0IL", row);
        row = getRow(0, "alarm");
        assertEquals("alWusVry77,body match - string not found,enPUVwV0IL,ac5rNRE5nk,chUWmQjirR,false," +
                        "npTechnicalContactsEmail,1364684157128,1364684157128,template_name=body match - string not " +
                        "found",
                row);
    }

    @Test
    public void testLine2() throws IOException {
        String row = getRow(1, "notification");
        assertEquals("ac5rNRE5nk:enPUVwV0IL:alNtjPwmDG:chUWmQjirR:1396739820000:CRITICAL," +
                "6be23b20-bd18-11e3-b474-eb57b6f62ca9,487439,enPUVwV0IL,chUWmQjirR,remote.http,alNtjPwmDG," +
                "www.everloop.com,1396739833810,4,5,2014,http server responding with 5xx status,critical," +
                "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
                "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
                "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP server " +
                "responding with 5xx status\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP server is functioning " +
                "normally\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP server responding with 5xx " +
                "status\u00041396739787101\u0004", row);
    }

    @Test
    public void testLine3() throws IOException {
        String row = getRow(2, "notification");
        assertEquals("acXUdnjOZu:enUcBkcgE6:alLtsNcS2F:chUVptIfeO:1396739820000:WARNING," +
                "6fa86ae0-bd18-11e3-bb54-5839dd8ab955,380954,enUcBkcgE6,chUVptIfeO,agent.cpu,alLtsNcS2F,," +
                "1396739840142,4,5,2014,cpu usage is 91.28%,warning,idle_percent_average=n\u00048" +
                ".7191190924387\u0004percent|stolen_percent_average=n\u00040\u0004percent|sys_percent_average=n" +
                "\u000446.154358726615\u0004percent|min_cpu_usage=n\u000489" +
                ".475087041263\u0004percent|max_cpu_usage=n\u000493" +
                ".086674773859\u0004percent|user_percent_average=n\u000445" +
                ".126522180946\u0004percent|irq_percent_average=n\u00040\u0004percent|wait_percent_average=n\u00040" +
                "\u0004percent|usage_average=n\u000491.280880907561\u0004percent,,,,false,," +
                "\u0004WARNING\u0004CPU usage is 91.28%\u00041396739840142\u0004", row);

        row = getRow(2, "entity");
        assertEquals("enUcBkcgE6,,web01-cj-dixcart-net,false,https://servers.api.rackspacecloud.com/v1" +
                ".0/380954/servers/836649,web01,1344624087984,1369404941972,public0_v4=173.203.192.234|private0_v4=10" +
                ".177.96.74,", row);
        row = getRow(2, "check");
        assertEquals("chUVptIfeO,enUcBkcgE6,acXUdnjOZu,cpu,agent.cpu,30,60,,,,false,1369405121593,1369405121593,,,," +
                "enUcBkcgE6", row);
        row = getRow(2, "alarm");
        assertEquals("alLtsNcS2F,cpu usage,enUcBkcgE6,acXUdnjOZu,chUVptIfeO,false,npTechnicalContactsEmail," +
                "1369405124516,1369405124516,template_name=agent.cpu_usage_average", row);
    }

    @Test
    public void testLine4() throws IOException {
        String row = getRow(3, "notification");
        assertEquals("acH8q8sevd:enDl25fGXB:alVdm0bzlR:chZb4EfLaQ:1396739820000:CRITICAL," +
                "733402a0-bd18-11e3-a5ee-5839dd8ab955,810442,enDl25fGXB,chZb4EfLaQ,agent.network,alVdm0bzlR,," +
                "1396739846090,4,5,2014,network receive rate on eth1 has exceeded 39321600 bytes/second,critical," +
                "tx_errors=l\u00040\u0004errors|rx_frame=l\u00040\u0004frames|tx_carrier=l\u00040\u0004errors" +
                "|rx_dropped=l\u00040\u0004packets|rx_packets=l\u000419015797720\u0004packets|tx_dropped=l\u00040" +
                "\u0004packets|tx_overruns=l\u00040\u0004overruns|tx_collisions=l\u00040\u0004collisions|rx_errors=l" +
                "\u00040\u0004errors|tx_bytes=l\u00044856437275426\u0004bytes|tx_packets=l\u000415135766135" +
                "\u0004packets|rx_overruns=l\u00040\u0004overruns|rx_bytes=l\u00041913727831859\u0004bytes,,,,false,," +
                "\u0004CRITICAL\u0004Network receive rate on eth1 has exceeded 39321600 " +
                "bytes/second\u00041396739846090\u0004", row);
    }

    @Test
    public void testLine5() throws IOException {
        String row = getRow(4, "notification");
        assertEquals("acH8q8sevd:enDl25fGXB:aleyXMLsWA:chZb4EfLaQ:1396739820000:CRITICAL," +
                "733402a1-bd18-11e3-85ab-5839dd8ab955,810442,enDl25fGXB,chZb4EfLaQ,agent.network,aleyXMLsWA,," +
                "1396739846090,4,5,2014,network receive rate on eth1 has exceeded 39321600 bytes/second,critical," +
                "tx_errors=l\u00040\u0004errors|rx_frame=l\u00040\u0004frames|tx_carrier=l\u00040\u0004errors" +
                "|rx_dropped=l\u00040\u0004packets|rx_packets=l\u000419015797720\u0004packets|tx_dropped=l\u00040" +
                "\u0004packets|tx_overruns=l\u00040\u0004overruns|tx_collisions=l\u00040\u0004collisions|rx_errors=l" +
                "\u00040\u0004errors|tx_bytes=l\u00044856437275426\u0004bytes|tx_packets=l\u000415135766135" +
                "\u0004packets|rx_overruns=l\u00040\u0004overruns|rx_bytes=l\u00041913727831859\u0004bytes,,,,false,," +
                "\u0004CRITICAL\u0004Network receive rate on eth1 has exceeded 39321600 " +
                "bytes/second\u00041396739846090\u0004", row);
    }

    @Test
    public void testLine6() throws IOException {
        String row = getRow(5, "notification");
        assertEquals("acq3svfnjb:eneKTASo0W:al2DHFvuOJ:chIiAN7FS2:1396739840000:WARNING," +
                "70559ad0-bd18-11e3-8c73-84d60c371d26,566572,eneKTASo0W,chIiAN7FS2,remote.http,al2DHFvuOJ," +
                "www.datingsitesontheweb.com,1396739841277,4,5,2014,http request took more than 20000 milliseconds.," +
                "warning,duration=i\u000421212\u0004milliseconds|tt_firstbyte=i\u000421212\u0004milliseconds|bytes=i" +
                "\u000419560\u0004bytes|tt_connect=i\u000416379\u0004milliseconds|code=s\u0004200\u0004unknown" +
                "|truncated=i\u00040\u0004bytes,,,,false,,mzdfw\u0004WARNING\u0004HTTP request took more than 20000 " +
                "milliseconds.\u00041396739841277\u0004|mzlon\u0004OK\u0004HTTP connection time is " +
                "normal\u00041396739778047\u0004|mzord\u0004CRITICAL\u0004timeout\u00041396739817002\u0004", row);
    }

    @Test
    public void testLine7() throws IOException {
        String row = getRow(6, "notification");
        assertEquals("acga9LhDC8:enWHUkCBlR:aldByDhExK:chKLxErj9K:1396739820000:WARNING," +
                "73471570-bd18-11e3-9718-ec33cfc8d116,10017436,enWHUkCBlR,chKLxErj9K,agent.cpu,aldByDhExK,," +
                "1396739846215,4,5,2014,cpu usage is 94.72%\\, above your warning threshold of 90%,warning," +
                "idle_percent_average=n\u00045.2763429214877\u0004percent|stolen_percent_average=n\u00040" +
                ".085947589756584\u0004percent|sys_percent_average=n\u000448" +
                ".881865879924\u0004percent|min_cpu_usage=n\u000492.83038147139\u0004percent|max_cpu_usage=n\u000496" +
                ".616932685635\u0004percent|user_percent_average=n\u000445" +
                ".660902397102\u0004percent|irq_percent_average=n\u00040\u0004percent|wait_percent_average=n\u00040" +
                ".094941211730425\u0004percent|usage_average=n\u000494.723657078512\u0004percent,,,,false,," +
                "\u0004WARNING\u0004CPU usage is 94.72%, above your warning threshold of " +
                "90%\u00041396739846215\u0004", row);
    }

    @Test
    public void testLine8() throws IOException {
        String row = getRow(7, "notification");
        assertEquals("ac8eau4JEE:enOWSAgSba:alk91AJ57O:chGQPJxXee:1396739850000:CRITICAL," +
                "77bd0d30-bd18-11e3-beff-5839dd8ab955,422772,enOWSAgSba,chGQPJxXee,remote.http,alk91AJ57O," +
                "monitoring.api.rackspace.com,1396739853699,4,5,2014,content not found\\, " +
                "critical error encountered.,critical," +
                "duration=i\u00046\u0004milliseconds|tt_firstbyte=i\u00046\u0004milliseconds|bytes=i\u00044" +
                "\u0004bytes|tt_connect=i\u00042\u0004milliseconds|code=s\u0004200\u0004unknown|truncated=i\u00040" +
                "\u0004bytes|body_match_content=s\u0004\u0004unknown,,,,false,,mzord\u0004CRITICAL\u0004Content not " +
                "found, CRITICAL error encountered.\u00041396739853699\u0004|mzlon\u0004OK\u0004Matched default " +
                "return statement\u00041396739815012\u0004|mzdfw\u0004CRITICAL\u0004Content not found, " +
                "CRITICAL error encountered.\u00041396739830550\u0004", row);
    }

    @Test
    public void testLine9() throws IOException {
        String row = getRow(8, "notification");
        assertEquals("acdVbVbavF:en8ZsnICHv:alf65AzCwz:chsz0VE8zF:1396739820000:OK," +
                "7b0f1e60-bd18-11e3-abe4-7da3f1c39fcc,901144,en8ZsnICHv,chsz0VE8zF,agent.load_average,alf65AzCwz,," +
                "1396739859270,4,5,2014,5 minute load average is 1\\, below your warning threshold of 1,ok," +
                "15m=n\u00041\u0004unknown|1m=n\u00040.78\u0004unknown|5m=n\u00041\u0004unknown,,,,false,," +
                "\u0004OK\u00045 minute load average is 1, below your warning threshold of " +
                "1\u00041396739859270\u0004", row);
    }

    @Test
    public void testLine10() throws IOException {
        String row = getRow(9, "notification");
        assertEquals("acH8q8sevd:enDl25fGXB:aleyXMLsWA:chZb4EfLaQ:1396739820000:OK," +
                "7d8108c0-bd18-11e3-84aa-eb57b6f62ca9,810442,enDl25fGXB,chZb4EfLaQ,agent.network,aleyXMLsWA,," +
                "1396739863372,4,5,2014,network transmit rate on eth1 is less than 32768000 bytes/second,ok," +
                "tx_errors=l\u00040\u0004errors|rx_frame=l\u00040\u0004frames|tx_carrier=l\u00040\u0004errors" +
                "|rx_dropped=l\u00040\u0004packets|rx_packets=l\u0004281573328\u0004packets|tx_dropped=l\u00040" +
                "\u0004packets|tx_overruns=l\u00040\u0004overruns|tx_collisions=l\u00040\u0004collisions|rx_errors=l" +
                "\u00040\u0004errors|tx_bytes=l\u000429980625784\u0004bytes|tx_packets=l\u0004122096718\u0004packets" +
                "|rx_overruns=l\u00040\u0004overruns|rx_bytes=l\u000431555780010\u0004bytes,,,,false,," +
                "\u0004OK\u0004Network transmit rate on eth1 is less than 32768000 " +
                "bytes/second\u00041396739863372\u0004", row);
    }

    @Test
    public void testNotificationWithoutMetrics() throws IOException {
        String row = getRow(10, "notification");
        assertEquals("acJVrfzjPT:enGu2RXb21:alHHHQAgC3:chbfDqVo8K:1396739860000:CRITICAL," +
                "85044080-bd18-11e3-8ea9-7da3f1c39fcc,848009,enGu2RXb21,chbfDqVo8K,remote.http,alHHHQAgC3," +
                "gamenationparks.com,1396739875976,4,5,2014,timeout,critical,,,,false,," +
                "mzord\u0004CRITICAL\u0004timeout\u00041396739875976\u0004|mzdfw\u0004OK\u0004HTTP connection time is" +
                " normal\u00041396739813994\u0004|mzlon\u0004CRITICAL\u0004timeout\u00041396739873315\u0004", row);
    }

    @Test
    public void testNoEntity() throws IOException {
        String json = "{\"event_id\":\"ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL\"," +
                "\"log_entry_id\":\"6be23b20-bd18-11e3-9fae-eb57b6f62ca9\",\"details\":{\"target\":\"www.everloop" +
                ".com\",\"timestamp\":1396739833810,\"metrics\":{\"tt_firstbyte\":{\"type\":\"I\",\"data\":21196," +
                "\"unit\":\"milliseconds\"},\"duration\":{\"type\":\"I\",\"data\":21196,\"unit\":\"milliseconds\"}," +
                "\"bytes\":{\"type\":\"i\",\"data\":166,\"unit\":\"bytes\"},\"tt_connect\":{\"type\":\"I\"," +
                "\"data\":100,\"unit\":\"milliseconds\"},\"body_match\":{\"type\":\"s\",\"data\":\"\"," +
                "\"unit\":\"unknown\"},\"truncated\":{\"type\":\"I\",\"data\":0,\"unit\":\"bytes\"}," +
                "\"code\":{\"type\":\"s\",\"data\":\"502\",\"unit\":\"unknown\"}},\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\"," +
                "\"txn_id\":\"txn.fix-moved-checks.overlord.t-1383153068431\",\"collector_address_v4\":\"78.136.44" +
                ".8\",\"collector_address_v6\":\"2a00:1a48:7902:0001:0000:0000:4e88:2c08\"," +
                "\"observations\":[{\"monitoring_zone_id\":\"mzlon\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739833810}," +
                "{\"monitoring_zone_id\":\"mzord\",\"state\":\"OK\",\"status\":\"HTTP response contains the correct " +
                "content\",\"timestamp\":1396739786574},{\"monitoring_zone_id\":\"mzdfw\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739787101}]}," +
                "\"check\":{\"id\":\"chUWmQjirR\",\"label\":\"www.everloop.com\",\"type\":\"remote.http\"," +
                "\"details\":{\"url\":\"http://www.everloop.com\",\"body\":\"Kids Privacy Policy\"," +
                "\"method\":\"GET\",\"auth_user\":null,\"auth_password\":null,\"follow_redirects\":true," +
                "\"payload\":null,\"include_body\":false},\"monitoring_zones_poll\":[\"mzdfw\",\"mzlon\",\"mzord\"]," +
                "\"timeout\":30,\"period\":60,\"target_alias\":null,\"target_hostname\":\"www.everloop.com\"," +
                "\"target_resolver\":\"IPv4\",\"disabled\":false,\"metadata\":null,\"created_at\":1364684154472," +
                "\"updated_at\":1383153071044},\"alarm\":{\"id\":\"alWusVry77\",\"label\":\"Body match - string not " +
                "found\",\"check_id\":\"chUWmQjirR\",\"criteria\":\"if (metric['body_match'] == '') {\\n  return new " +
                "AlarmStatus(CRITICAL, 'HTTP response did not contain the correct content.');\\n}\\n\\nreturn new " +
                "AlarmStatus(OK, 'HTTP response contains the correct content');\\n\",\"disabled\":false," +
                "\"notification_plan_id\":\"npTechnicalContactsEmail\",\"metadata\":{\"template_name\":\"Body match -" +
                " string not found\"},\"created_at\":1364684157128,\"updated_at\":1364684157128}," +
                "\"tenant_id\":\"487439\"}";
        Notification m = new JacksonConverter<Notification>(Notification.class).convertFrom(json);
        MappedRows rows = cvt.convertFrom(m);
        String row = (String) ((List) rows.getRows().get("notification")).get(0);
        assertEquals("ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL," +
                "6be23b20-bd18-11e3-9fae-eb57b6f62ca9,487439,,chUWmQjirR,remote.http,alWusVry77,www.everloop.com," +
                "1396739833810,4,5,2014,http response did not contain the correct content.,critical," +
                "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
                "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
                "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP response did" +
                " not contain the correct content.\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP response contains" +
                " the correct content\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP response did not contain" +
                " the correct content.\u00041396739787101\u0004", row);
    }

    @Test
    public void testNoAlarm() throws IOException {
        String json = "{\"event_id\":\"ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL\"," +
                "\"log_entry_id\":\"6be23b20-bd18-11e3-9fae-eb57b6f62ca9\",\"details\":{\"target\":\"www.everloop" +
                ".com\",\"timestamp\":1396739833810,\"metrics\":{\"tt_firstbyte\":{\"type\":\"I\",\"data\":21196," +
                "\"unit\":\"milliseconds\"},\"duration\":{\"type\":\"I\",\"data\":21196,\"unit\":\"milliseconds\"}," +
                "\"bytes\":{\"type\":\"i\",\"data\":166,\"unit\":\"bytes\"},\"tt_connect\":{\"type\":\"I\"," +
                "\"data\":100,\"unit\":\"milliseconds\"},\"body_match\":{\"type\":\"s\",\"data\":\"\"," +
                "\"unit\":\"unknown\"},\"truncated\":{\"type\":\"I\",\"data\":0,\"unit\":\"bytes\"}," +
                "\"code\":{\"type\":\"s\",\"data\":\"502\",\"unit\":\"unknown\"}},\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\"," +
                "\"txn_id\":\"txn.fix-moved-checks.overlord.t-1383153068431\",\"collector_address_v4\":\"78.136.44" +
                ".8\",\"collector_address_v6\":\"2a00:1a48:7902:0001:0000:0000:4e88:2c08\"," +
                "\"observations\":[{\"monitoring_zone_id\":\"mzlon\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739833810}," +
                "{\"monitoring_zone_id\":\"mzord\",\"state\":\"OK\",\"status\":\"HTTP response contains the correct " +
                "content\",\"timestamp\":1396739786574},{\"monitoring_zone_id\":\"mzdfw\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739787101}]}," +
                "\"entity\":{\"id\":\"enPUVwV0IL\",\"label\":\"everloop-load-balancer\"," +
                "\"ip_addresses\":{\"public0_v4\":\"173.203.51.14\",\"private0_v4\":\"10.179.65.243\"}," +
                "\"metadata\":null,\"managed\":false,\"uri\":\"https://servers.api.rackspacecloud.com/v1" +
                ".0/487439/servers/202936\",\"agent_id\":null,\"created_at\":1344564747355," +
                "\"updated_at\":1344564747355},\"check\":{\"id\":\"chUWmQjirR\",\"label\":\"www.everloop.com\"," +
                "\"type\":\"remote.http\",\"details\":{\"url\":\"http://www.everloop.com\"," +
                "\"body\":\"Kids Privacy Policy\",\"method\":\"GET\",\"auth_user\":null,\"auth_password\":null," +
                "\"follow_redirects\":true,\"payload\":null,\"include_body\":false}," +
                "\"monitoring_zones_poll\":[\"mzdfw\",\"mzlon\",\"mzord\"],\"timeout\":30,\"period\":60," +
                "\"target_alias\":null,\"target_hostname\":\"www.everloop.com\",\"target_resolver\":\"IPv4\"," +
                "\"disabled\":false,\"metadata\":null,\"created_at\":1364684154472,\"updated_at\":1383153071044}," +
                "\"created_at\":1364684157128,\"updated_at\":1364684157128},\"tenant_id\":\"487439\"}";

        Notification m = new JacksonConverter<Notification>(Notification.class).convertFrom(json);
        MappedRows rows = cvt.convertFrom(m);
        String row = (String) ((List) rows.getRows().get("notification")).get(0);
        assertEquals("ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL," +
                "6be23b20-bd18-11e3-9fae-eb57b6f62ca9,,enPUVwV0IL,chUWmQjirR,remote.http,,www.everloop.com," +
                "1396739833810,4,5,2014,http response did not contain the correct content.,critical," +
                "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
                "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
                "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP response did" +
                " not contain the correct content.\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP response contains" +
                " the correct content\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP response did not contain" +
                " the correct content.\u00041396739787101\u0004", row);
    }

    @Test
    public void testNoCheck() throws IOException {
        String json = "{\"event_id\":\"ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL\"," +
                "\"log_entry_id\":\"6be23b20-bd18-11e3-9fae-eb57b6f62ca9\",\"details\":{\"target\":\"www.everloop" +
                ".com\",\"timestamp\":1396739833810,\"metrics\":{\"tt_firstbyte\":{\"type\":\"I\",\"data\":21196," +
                "\"unit\":\"milliseconds\"},\"duration\":{\"type\":\"I\",\"data\":21196,\"unit\":\"milliseconds\"}," +
                "\"bytes\":{\"type\":\"i\",\"data\":166,\"unit\":\"bytes\"},\"tt_connect\":{\"type\":\"I\"," +
                "\"data\":100,\"unit\":\"milliseconds\"},\"body_match\":{\"type\":\"s\",\"data\":\"\"," +
                "\"unit\":\"unknown\"},\"truncated\":{\"type\":\"I\",\"data\":0,\"unit\":\"bytes\"}," +
                "\"code\":{\"type\":\"s\",\"data\":\"502\",\"unit\":\"unknown\"}},\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\"," +
                "\"txn_id\":\"txn.fix-moved-checks.overlord.t-1383153068431\",\"collector_address_v4\":\"78.136.44" +
                ".8\",\"collector_address_v6\":\"2a00:1a48:7902:0001:0000:0000:4e88:2c08\"," +
                "\"observations\":[{\"monitoring_zone_id\":\"mzlon\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739833810}," +
                "{\"monitoring_zone_id\":\"mzord\",\"state\":\"OK\",\"status\":\"HTTP response contains the correct " +
                "content\",\"timestamp\":1396739786574},{\"monitoring_zone_id\":\"mzdfw\",\"state\":\"CRITICAL\"," +
                "\"status\":\"HTTP response did not contain the correct content.\",\"timestamp\":1396739787101}]}," +
                "\"entity\":{\"id\":\"enPUVwV0IL\",\"label\":\"everloop-load-balancer\"," +
                "\"ip_addresses\":{\"public0_v4\":\"173.203.51.14\",\"private0_v4\":\"10.179.65.243\"}," +
                "\"metadata\":null,\"managed\":false,\"uri\":\"https://servers.api.rackspacecloud.com/v1" +
                ".0/487439/servers/202936\",\"agent_id\":null,\"created_at\":1344564747355," +
                "\"updated_at\":1344564747355},\"alarm\":{\"id\":\"alWusVry77\",\"label\":\"Body match - string not " +
                "found\",\"check_id\":\"chUWmQjirR\",\"criteria\":\"if (metric['body_match'] == '') {\\n  return new " +
                "AlarmStatus(CRITICAL, 'HTTP response did not contain the correct content.');\\n}\\n\\nreturn new " +
                "AlarmStatus(OK, 'HTTP response contains the correct content');\\n\",\"disabled\":false," +
                "\"notification_plan_id\":\"npTechnicalContactsEmail\",\"metadata\":{\"template_name\":\"Body match -" +
                " string not found\"},\"created_at\":1364684157128,\"updated_at\":1364684157128}," +
                "\"tenant_id\":\"487439\"}";
        Notification m = new JacksonConverter<Notification>(Notification.class).convertFrom(json);
        MappedRows rows = cvt.convertFrom(m);
        String row = (String) ((List) rows.getRows().get("notification")).get(0);
        assertEquals("ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL," +
                "6be23b20-bd18-11e3-9fae-eb57b6f62ca9,487439,enPUVwV0IL,,,alWusVry77,www.everloop.com,1396739833810," +
                "4,5,2014,http response did not contain the correct content.,critical," +
                "tt_firstbyte=i\u000421196\u0004milliseconds|duration=i\u000421196\u0004milliseconds|bytes=i\u0004166" +
                "\u0004bytes|tt_connect=i\u0004100\u0004milliseconds|body_match=s\u0004\u0004unknown|truncated=i" +
                "\u00040\u0004bytes|code=s\u0004502\u0004unknown,,,,false,,mzlon\u0004CRITICAL\u0004HTTP response did" +
                " not contain the correct content.\u00041396739833810\u0004|mzord\u0004OK\u0004HTTP response contains" +
                " the correct content\u00041396739786574\u0004|mzdfw\u0004CRITICAL\u0004HTTP response did not contain" +
                " the correct content.\u00041396739787101\u0004", row);
    }

    @Test
    public void whenUsingEntitySuppressionFields() throws IOException {
        String row = getRow(12, "notification");
        assertEquals("acNWKaSrd4:en9Oe8hgPm:al8w4fTKJo:chtVX3jWfb:1428191940000:OK," +
                "ae861320-db26-11e4-905f-41211b0ca7e9,632441,en9Oe8hgPm,chtVX3jWfb,agent.memory,al8w4fTKJo,," +
                "1428191991122,4,4,2015,memory usage is below 90%,ok," +
                "total=l\u00042096967680\u0004bytes|swap_used=l\u0004720289792\u0004bytes|free=l\u0004234545152" +
                "\u0004bytes|swap_free=l\u00043573301248\u0004bytes|ram=l\u00042000\u0004megabytes|swap_page_out=l" +
                "\u0004740389\u0004bytes|actual_used=l\u00041844584448\u0004bytes|actual_free=l\u0004252383232" +
                "\u0004bytes|swap_page_in=l\u00041074920\u0004bytes|used=l\u00041862422528\u0004bytes|swap_total=l" +
                "\u00044293591040\u0004bytes,,,e123|etester,false,ntTechnicalContact1|ntTechnicalContact2," +
                "\u0004OK\u0004Memory usage is below 90%\u00041428191991122\u0004UP", row);
    }

    @Test
    public void whenUsingAllSuppressionFields() throws IOException {
        String row = getRow(13, "notification");
        assertEquals("acld4PzHch:en0vs0Zq4n:alvHamF71v:chapNlj61m:1428191940000:CRITICAL," +
                "ac273b90-db26-11e4-871b-971820dc224d,917165,en0vs0Zq4n,chapNlj61m,remote.http,alvHamF71v," +
                "prod-000-cron-000.levi-site.com,1428191987145,4,4,2015,timeout,critical,a1|a2,c1|c2,e1|e2,true," +
                "ntManaged1|ntManaged2," +
                "mzord\u0004CRITICAL\u0004timeout\u00041428191987145\u0004UP|mzlon\u0004OK\u0004HTTP server is " +
                "functioning normally\u00041428191920767\u0004UP|mzdfw\u0004CRITICAL\u0004timeout\u00041428191966114" +
                "\u0004UP", row);
    }

    @Test
    public void whenPassingExistingMappedRow() throws IOException {
        Notification m = new JacksonConverter<Notification>(Notification.class).convertFrom(notifications.get(0));
        MappedRows rows = new MappedRows();
        MappedRows nrows = cvt.convertFrom(m, rows);
        assertSame(nrows, rows);
    }
}
