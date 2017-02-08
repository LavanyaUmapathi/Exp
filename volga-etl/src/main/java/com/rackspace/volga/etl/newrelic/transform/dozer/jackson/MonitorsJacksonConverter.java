package com.rackspace.volga.etl.newrelic.transform.dozer.jackson;

import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.newrelic.dto.json.Monitor;

public class MonitorsJacksonConverter extends JacksonConverter<Monitor> {
    public MonitorsJacksonConverter() {
        super(Monitor.class);
    }
}
