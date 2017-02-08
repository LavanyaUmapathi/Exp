package com.rackspace.volga.etl.newrelic.transform.dozer.jackson;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.rackspace.volga.etl.newrelic.dto.json.Monitor;


public class MonitorsJacksonConverterTest {

    @Test
    public void loadMonitor() throws IOException {
        List<String> monitorLines = Files.readLines(new ClassPathResource("newrelic/monitors.json").getFile(),
                Charset.defaultCharset());        
        String monitorText = Joiner.on(' ').join(monitorLines);
        
        Monitor monitor = new MonitorsJacksonConverter().convertFrom(monitorText);
        assertEquals("40627039-a9c5-45b0-a7ff-525a73e9a500", monitor.getId());
        assertEquals("ENABLED", monitor.getStatus());
        assertEquals("681190-avreporting-web-monitor", monitor.getName());
        assertEquals("2016-05-06T17:44:07.431+0000", monitor.getCreatedAt());
        assertEquals("0", monitor.getModifiedAt());
        assertEquals(1440, monitor.getFrequency());
        assertEquals("Dallas, TX",monitor.getLocation());
        assertEquals("681190", monitor.getDevice());
        assertEquals("999999", monitor.getAccount());
        assertEquals("SCRIPT_BROWSER", monitor.getType());
        assertEquals("to be determined", monitor.getFriendlyName());
    }
}
