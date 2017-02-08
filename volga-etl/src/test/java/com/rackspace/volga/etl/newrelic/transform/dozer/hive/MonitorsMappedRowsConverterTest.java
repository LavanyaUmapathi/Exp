package com.rackspace.volga.etl.newrelic.transform.dozer.hive;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.Test;

import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.newrelic.dto.json.Monitor;

public class MonitorsMappedRowsConverterTest {

	@Test
	public void testConvertFromMonitorMappedRows() {
		Monitor monitor = new Monitor();
        monitor.setId("40627039-a9c5-45b0-a7ff-525a73e9a500");
        monitor.setStatus("ENABLED");
        monitor.setName("681190-avreporting-web-monitor");
        monitor.setCreatedAt("2016-05-06T17:44:07.000+0000");
        monitor.setModifiedAt("0");
        monitor.setFrequency(1440);
        monitor.setLocation("Dallas, TX");
        monitor.setDevice("681190");
        monitor.setAccount("999999");
        monitor.setType("SCRIPT_BROWSER");
        monitor.setFriendlyName("to be determined");
				
		MappedRows rows = new MonitorsMappedRowsConverter().convertFrom(monitor);
		assertEquals(1, rows.getRows().size());
		
		Collection<String> mappedRows = rows.getRows().get(MonitorsMappedRowsConverter.MONITOR_KEY);
		String firstRow = mappedRows.iterator().next();

		long createdAt = 1462556647000L;
		long modifiedAt = 0;
		String location = HiveFormatUtils.sanitize(monitor.getLocation());

		StringBuilder expected = new StringBuilder();
		expected
			.append(monitor.getId())
			.append(HiveFormatUtils.FIELD_SEP_CHAR)
			.append(monitor.getStatus())
			.append(HiveFormatUtils.FIELD_SEP_CHAR)
			.append(monitor.getName())
			.append(HiveFormatUtils.FIELD_SEP_CHAR)
			.append(createdAt)
			.append(HiveFormatUtils.FIELD_SEP_CHAR)        
			.append(modifiedAt)
			.append(HiveFormatUtils.FIELD_SEP_CHAR)
			.append(monitor.getFrequency())
			.append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(location)
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)        
	        .append(monitor.getDevice())
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(MonitorsMappedRowsConverter.SOURCE_SYSTEM)
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(monitor.getAccount())
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(MonitorsMappedRowsConverter.SOURCE_SYSTEM)
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(monitor.getType())
	        .append(HiveFormatUtils.FIELD_SEP_CHAR)
	        .append(monitor.getFriendlyName());
		
		assertEquals(expected.toString(), firstRow);
	}

}
