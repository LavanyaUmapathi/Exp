package com.rackspace.volga.etl.newrelic.transform.dozer.hive;

import java.util.List;

import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.newrelic.dto.json.Monitor;

public class MonitorsMappedRowsConverter extends NewRelicMappedRowsConverter<Monitor> {

	static final String SOURCE_SYSTEM = "MANAGED_HOSTING";
	
    public MonitorsMappedRowsConverter() {
        super(MappedRows.class, Monitor.class);
    }

    @Override
    public MappedRows convertFrom(Monitor source, MappedRows rows) {
        if (rows == null) {
            rows = new MappedRows();
        }
        String monitorString = getMonitorFields(source);
        rows.addRow(MONITOR_KEY, monitorString);
        return rows;
    }

    private static String getMonitorFields(Monitor m) {
        
		List<String> baseFields = Lists.newArrayList(
                m.getId(),
                m.getStatus(),
                m.getName(),
                String.valueOf(getMillis(m.getCreatedAt())),
                String.valueOf(getMillis(m.getModifiedAt())),
                String.valueOf(m.getFrequency()),
                HiveFormatUtils.sanitize(m.getLocation()),
                m.getDevice(),
                SOURCE_SYSTEM,
                m.getAccount(),
                SOURCE_SYSTEM,
                m.getType(),
                HiveFormatUtils.sanitize(m.getFriendlyName())
        );

        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(EMPTY).join(baseFields);
    }
    
    private static long getMillis(String isoDateTimeString) {
    	try {
			long millis = DateTime.parse(isoDateTimeString).getMillis();
			return (millis > 0) ? millis : 0;
		} catch (IllegalArgumentException e) {
			return 0;
		}
    }
}
