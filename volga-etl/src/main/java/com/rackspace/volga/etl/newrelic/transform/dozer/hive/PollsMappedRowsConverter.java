package com.rackspace.volga.etl.newrelic.transform.dozer.hive;

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.newrelic.dto.json.Poll;
import com.rackspace.volga.etl.newrelic.dto.json.Polls;

public class PollsMappedRowsConverter extends NewRelicMappedRowsConverter<Polls> {

	private static final DateTimeFormatter formatter = 
			DateTimeFormat.forPattern("yyyy-MM-dd");

    public PollsMappedRowsConverter() {
        super(MappedRows.class, Polls.class);
    }

    @Override
    public MappedRows convertFrom(Polls source, MappedRows rows) {
        if (rows == null) {
            rows = new MappedRows();
        }
        
        for (Poll poll: source.getPolls()) {
	        rows.addRow(POLL_KEY, getPollFields(poll, source.getId()));
        }
        return rows;
    }

    private static String getPollFields(Poll p, String monitorId) {
        List<String> baseFields = Lists.newArrayList(
                monitorId,
                String.valueOf(p.getTime()),
                "SUCCESS".equals(p.getStatus()) ? "true" : "false",
        		formatter.print(new DateTime(p.getTime()))
        );

        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(EMPTY).join(baseFields);
    }
}
