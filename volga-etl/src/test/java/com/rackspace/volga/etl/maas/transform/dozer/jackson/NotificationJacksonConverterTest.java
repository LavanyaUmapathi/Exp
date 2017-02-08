package com.rackspace.volga.etl.maas.transform.dozer.jackson;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.rackspace.volga.etl.maas.dto.json.*;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 4/14/15
 * Time: 9:40 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class NotificationJacksonConverterTest {

    @Test
    public void loadNotifications() throws IOException {

        String notifications = Files.readFirstLine(new ClassPathResource("maas/notifications.json").getFile(),
                Charset.defaultCharset());
        NotificationJacksonConverter t = new NotificationJacksonConverter();
        Notification notification = t.convertFrom(notifications);
        assertEquals(notification.getEventId(), "ac5rNRE5nk:enPUVwV0IL:alWusVry77:chUWmQjirR:1396739820000:CRITICAL");
        assertEquals(notification.getLogEntryId(), "6be23b20-bd18-11e3-9fae-eb57b6f62ca9");
        Details d = notification.getDetails();
        assertEquals(d.getTarget(), "www.everloop.com");
        assertEquals(d.getTimestamp(), 1396739833810L);
        Map<String, Map<String, Object>> m = d.getMetrics();
        assertEquals(m.size(), 7);
        assertEquals(m.get("tt_firstbyte").get("type"), "I");
        assertEquals(m.get("tt_firstbyte").get("data"), 21196);
        assertEquals(m.get("tt_firstbyte").get("unit"), "milliseconds");
        assertEquals(d.getState(), "CRITICAL");
        assertEquals(d.getStatus(), "HTTP response did not contain the correct content.");
        assertEquals(d.getTxnId(), "txn.fix-moved-checks.overlord.t-1383153068431");
        List<Observation> obs = d.getObservations();
        assertEquals(obs.size(), 3);
        Observation o = obs.get(1);
        assertEquals(o.getMonitoring_zone_id(), "mzord");
        assertEquals(o.getState(), "OK");
        assertEquals(o.getStatus(), "HTTP response contains the correct content");
        Entity e = notification.getEntity();
        assertEquals(e.getId(), "enPUVwV0IL");
        assertEquals(e.getLabel(), "everloop-load-balancer");
        assertEquals(e.getIpAddresses().get("public0_v4"), "173.203.51.14");
        assertEquals(e.getIpAddresses().get("private0_v4"), "10.179.65.243");
        assertEquals(e.getIpAddresses().size(), 2);
        assertEquals(e.getMetadata(), null);
        assertEquals(e.isManaged(), false);
        assertEquals(e.getUri(), "https://servers.api.rackspacecloud.com/v1.0/487439/servers/202936");
        assertEquals(e.getAgent_id(), null);
        assertEquals(e.getCreated_at(), "1344564747355");
        assertEquals(e.getUpdated_at(), "1344564747355");
        Check c = notification.getCheck();
        assertEquals(c.getId(), "chUWmQjirR");
        assertEquals(c.getLabel(), "www.everloop.com");
        assertEquals(c.getType(), "remote.http");
        assertEquals(c.getDetails().get("url"), "http://www.everloop.com");
        assertEquals(c.getDetails().get("body"), "Kids Privacy Policy");
        assertEquals(c.getDetails().get("method"), "GET");
        assertEquals(c.getDetails().get("auth_user"), null);
        assertEquals(c.getDetails().get("follow_redirects"), true);
        assertEquals(c.getDetails().get("payload"), null);
        assertEquals(c.getDetails().get("include_body"), false);
        List<String> zones = c.getMonitoring_zones_poll();
        assertEquals(zones, Lists.newArrayList("mzdfw", "mzlon", "mzord"));
        assertEquals(c.getTimeout(), 30);
        assertEquals(c.getPeriod(), 60);
        assertEquals(c.getTarget_alias(), null);
        assertEquals(c.getTarget_hostname(), "www.everloop.com");
        assertEquals(c.getTarget_resolver(), "IPv4");
        assertEquals(c.isDisabled(), false);
        assertEquals(c.getMetadata(), null);
        assertEquals(c.getCreatedAt(), 1364684154472L);
        assertEquals(c.getUpdatedAt(), 1383153071044L);
        Alarm a = notification.getAlarm();
        assertEquals(a.getId(), "alWusVry77");
        assertEquals(a.getLabel(), "Body match - string not found");
        assertEquals(a.getCheckId(), "chUWmQjirR");
        assertEquals(a.isDisabled(), false);
        assertEquals(a.getNotificationPlanId(), "npTechnicalContactsEmail");
        assertEquals(a.getLabel(), "Body match - string not found");
        assertEquals(a.getMetadata().get("template_name"), "Body match - string not found");
        assertEquals(a.getCreatedAt(), 1364684157128L);
        assertEquals(a.getUpdatedAt(), 1364684157128L);
        assertEquals(notification.getTenantId(), "487439");

    }

}
