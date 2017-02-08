package com.rackspace.volga.etl.newrelic.transform.dozer.jackson;

import com.rackspace.volga.etl.common.transform.JacksonConverter;
import com.rackspace.volga.etl.newrelic.dto.json.Polls;

public class PollsJacksonConverter extends JacksonConverter<Polls> {

	public PollsJacksonConverter() {
        super(Polls.class);
    }
}
