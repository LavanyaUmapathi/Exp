package com.rackspace.volga.etl.newrelic.transform.dozer.hive;

import org.apache.commons.lang.StringUtils;

import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.transform.BaseConverter;

public abstract class NewRelicMappedRowsConverter<T> extends BaseConverter<MappedRows,T> {

    protected static final String MONITOR_KEY = "monitor";

    protected static final String POLL_KEY = "polls";

    protected static final String EMPTY = StringUtils.EMPTY;

    protected NewRelicMappedRowsConverter(Class<MappedRows> prototypeA, Class<T> prototypeB) {
        super(prototypeA, prototypeB);
    }
}
