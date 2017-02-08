package com.rackspace.volga.etl.newrelic.transform.dozer.jackson;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.rackspace.volga.etl.newrelic.dto.json.Poll;
import com.rackspace.volga.etl.newrelic.dto.json.Polls;

public class PollsJacksonConverterTest {

	@Test
    public void loadPolls() throws IOException {
        List<String> pollsLines = Files.readLines(new ClassPathResource("newrelic/polls.json").getFile(),
                Charset.defaultCharset());        
        String pollsText = Joiner.on(' ').join(pollsLines);
        
        Polls polls = new PollsJacksonConverter().convertFrom(pollsText);
        assertEquals("40627039-a9c5-45b0-a7ff-525a73e9a500", polls.getId());
        assertEquals("2016-05-01", polls.getStart());
        assertEquals("2016-05-31", polls.getEnd());
        assertEquals(2, polls.getPolls().size());
        assertEquals(2, polls.getCount());
        
        Poll poll = polls.getPolls().get(0);
        assertEquals(1464283262000L, poll.getTime());
        assertEquals("SUCCESS", poll.getStatus());
        
        poll = polls.getPolls().get(1);
        assertEquals(1464283262999L, poll.getTime());
        assertEquals("FAILURE", poll.getStatus());
    }
}
