package com.rackspace.volga.etl.maas.transform.dozer.jackson;

import com.google.common.io.Files;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * User: alex.silva
 * Date: 4/14/15
 * Time: 9:40 AM
 * Copyright Rackspace Hosting, Inc.
 */
public class MetricsJacksonConverterTest {

    @Test
    public void loadMetrics() throws IOException {
        String metrics = Files.readFirstLine(new ClassPathResource("maas/metrics.json").getFile(),
                Charset.defaultCharset());
        MetricsJacksonConverter t = new MetricsJacksonConverter();
        Metric metric = t.convertFrom(metrics);
        assertEquals(metric.getAccountId(), "ac4ubIxzmW");
        assertEquals(metric.getVersion(), 1L);
        assertEquals(metric.getId(), null);
        assertEquals(metric.getTimestamp(), 1395941810615L);
        assertEquals(metric.getTenantId(), "898450");
        assertEquals(metric.getEntityId(), "enC6anIcNY");
        assertEquals(metric.getCheckId(), "chOdIdiZu0");
        assertEquals(metric.getDimensionKey(), null);
        assertEquals(metric.getTarget(), null);
        assertEquals(metric.getCheckType(), "agent.load_average");
        assertEquals(metric.getMonitoringZoneId(), null);
        assertEquals(metric.getCollectorId(), null);
        assertTrue(metric.isAvailable());
        Map<String, Map<String, Object>> m = metric.getMetrics();
        assertEquals(m.size(), 3);
        assertEquals(m.get("15m").get("metricType"), 110);
        assertEquals(m.get("15m").get("valueDbl"), 0.05d);
        assertEquals(m.get("15m").get("valueI64"), 0);
        assertEquals(m.get("15m").get("valueI32"), 0);
        assertEquals(m.get("15m").get("valueBool"), false);
        assertEquals(m.get("15m").get("valueStr"), null);
        assertEquals(m.get("15m").get("unitEnum"), "UNKNOWN");
        assertEquals(m.get("15m").get("unitOtherStr"), null);
    }

}
