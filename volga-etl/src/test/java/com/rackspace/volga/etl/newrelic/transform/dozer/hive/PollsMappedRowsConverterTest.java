package com.rackspace.volga.etl.newrelic.transform.dozer.hive;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.newrelic.dto.json.Poll;
import com.rackspace.volga.etl.newrelic.dto.json.Polls;

public class PollsMappedRowsConverterTest {

	@Test
	public void testConvertFromPollsMappedRows() {
		Polls polls = new Polls();
		polls.setId("40627039-a9c5-45b0-a7ff-525a73e9a500");
		polls.setStart("2016-05-01");
		polls.setEnd("2016-05-31");
		
		List<Poll> allPolls = Lists.newArrayList(
			new Poll(1464283262L, "SUCCESS"), //without msecs
			new Poll(1474471595911L, "FAILED"), //with msecs
			new Poll(1474471597000L, "FAILURE") //another non-SUCCESS word
		);
		polls.setPolls(allPolls);
		polls.setCount(allPolls.size());
				
		MappedRows rows = new PollsMappedRowsConverter().convertFrom(polls);
		assertEquals(1, rows.getRows().size());
		
		Collection<String> mappedRows = rows.getRows().get(PollsMappedRowsConverter.POLL_KEY);
		Iterator<String> rowsIterator = mappedRows.iterator();
		
		String nextRow = rowsIterator.next();
		String expected = "40627039-a9c5-45b0-a7ff-525a73e9a500,1464283262000,true,2016-05-26";
		assertEquals(expected, nextRow);
		
		nextRow = rowsIterator.next();
		expected = "40627039-a9c5-45b0-a7ff-525a73e9a500,1474471595911,false,2016-09-21";
		assertEquals(expected, nextRow);
		
		nextRow = rowsIterator.next();
		expected = "40627039-a9c5-45b0-a7ff-525a73e9a500,1474471597000,false,2016-09-21";
		assertEquals(expected, nextRow);
	}
}
