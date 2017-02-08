package com.rackspace.volga.etl.maas.transform.dozer.jackson;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.rackspace.volga.etl.maas.dto.json.Configuration;
import com.rackspace.volga.etl.maas.dto.json.Entity;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 4/14/15
 * Time: 9:40 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class ConfigurationJacksonConverterTest {

    @Test
    public void loadConfigurations() throws IOException {
        String configs = Files.readFirstLine(new ClassPathResource("maas/configurations.json").getFile(),
                Charset.defaultCharset());
        ConfigurationJacksonConverter t = new ConfigurationJacksonConverter();
        Configuration c = t.convertFrom(configs);
        assertEquals(c.getId(), "ac006xgyTj");
        assertEquals(c.getExternal_id(), "824378");
        assertEquals(c.getMetadata().getAdditionalProperties(), new HashMap<String, Object>());
        assertEquals(c.getMin_check_interval(), 30L);
        assertEquals(c.getWebhook_token(), "of43bh5n6fU6davEoA4ODolSoounl18U");
        assertEquals(c.getSoft_remove_ttl(), 5184000l);
        assertEquals(c.getAccount_status(), "GOOD");
        assertEquals(c.isRackspace_managed(), false);
        assertEquals(c.getCep_group_id(), "cgC");
        assertEquals(c.getApiRateLimits().get("global"), 50000);
        assertEquals(c.getApiRateLimits().get("test_check"), 500);
        assertEquals(c.getApiRateLimits().get("test_alarm"), 500);
        assertEquals(c.getApiRateLimits().get("test_notification"), 200);
        assertEquals(c.getApiRateLimits().get("traceroute"), 300);
        assertEquals(c.getLimits().getAlarms(), 1000);
        assertEquals(c.getLimits().getChecks(), 1000);
        assertEquals(c.getFeatures().getAdditionalProperties(), new HashMap<String, Object>());
        assertEquals(c.getAgent_bundle_channel(), "stable");
        assertEquals(c.getCheck_type_channel(), "stable");
        Map<String, Object> metrics = c.getMetricsTTL();
        assertEquals(metrics.size(), 6);
        assertEquals(metrics.get("full"), 48);
        assertEquals(c.getContacts(), null);
        assertEquals(c.getCheck_type_channel(), "stable");
        assertEquals(c.getEntities().size(), 1);
        Entity e = c.getEntities().get(0);
        assertEquals(e.getId(), "enRayzGfb6");
        assertEquals(e.getLabel(), "Zapmove");
        assertEquals(e.getIpAddresses().get("access_ip0_v6"), "2001:4801:7819:0074:ee84:d5c2:ff10:9c20");
        assertEquals(e.getIpAddresses().get("public0_v6"), "2001:4801:7819:0074:ee84:d5c2:ff10:9c20");
        assertEquals(e.getIpAddresses().get("access_ip1_v4"), "162.209.1.70");
        assertEquals(e.getIpAddresses().get("public1_v4"), "162.209.1.70");
        assertEquals(e.getMetadata(), null);
        assertEquals(e.isManaged(), false);
        assertEquals(e.getUri(), "https://ord.servers.api.rackspacecloud" +
                ".com/824378/servers/f3c9ef75-4899-4e77-88c1-ce2261b0b15a");
        assertEquals(e.getAgent_id(), null);
        assertEquals(e.getCreated_at(), "1366957208601");
        assertEquals(e.getUpdated_at(), "1366957208601");
        assertEquals(e.getChecks(), Lists.newArrayList());
        assertEquals(e.getAlarms(), Lists.newArrayList());
    }

}
