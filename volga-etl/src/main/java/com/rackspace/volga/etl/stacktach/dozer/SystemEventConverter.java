package com.rackspace.volga.etl.stacktach.dozer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.stacktach.dto.json.SystemEvent;
import org.dozer.CustomConverter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;

/**
 * User: alex.silva
 * Time: 12:38 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class SystemEventConverter implements CustomConverter {
    private static final String EVENT_KEY = "event";

    private static final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");


    @Override
    public Object convert(Object obj, Object source, Class<?> destinationClass, Class<?> sourceClass) {
        SystemEvent evt = (SystemEvent) source;
        MappedRows rows = new MappedRows();
        rows.addRow(EVENT_KEY, getEventFields(evt));
        return rows;
    }

    private String getEventFields(SystemEvent evt) {
        DateTime cTimestamp = new DateTime(evt.getContextTimestamp(), DateTimeZone.UTC);
        DateTime eTimestamp = fmt.parseDateTime(evt.getTimestamp()).withZoneRetainFields(DateTimeZone.UTC);
        ObjectMapper m = new ObjectMapper();
        m.setPropertyNamingStrategy(new PropertyNamingStrategy.LowerCaseWithUnderscoresStrategy());
        Map<String, Object> payload = m.convertValue(evt.getPayload(), Map.class);
        Map<String, Object> payloadMetadata = (Map<String, Object>) payload.get("metadata");
        payload.remove("metadata");
        Map<String, Object> bandwidth = (Map<String, Object>) payload.get("bandwidth");
        payload.remove("bandwidth");
        Map<String, Object> payloadImageMeta = (Map<String, Object>) payload.get("image_meta");
        payload.remove("image_meta");

        List<String> fields = Lists.newArrayList(
                evt.getContextRequestId(),
                String.valueOf(evt.getContextQuotaClass()),
                evt.getEventType(),
                HiveFormatUtils.mapToString(payload, HiveFormatUtils.MAP_SEP_CHAR,
                        HiveFormatUtils.COLLECTION_SEP_CHAR),
                HiveFormatUtils.mapToString(payloadMetadata,
                        HiveFormatUtils.MAP_SEP_CHAR, HiveFormatUtils.COLLECTION_SEP_CHAR),
                HiveFormatUtils.mapToString(bandwidth,
                        HiveFormatUtils.MAP_SEP_CHAR, HiveFormatUtils.COLLECTION_SEP_CHAR),
                HiveFormatUtils.mapToString(payloadImageMeta,
                        HiveFormatUtils.MAP_SEP_CHAR, HiveFormatUtils.COLLECTION_SEP_CHAR),
                evt.getPriority(),
                String.valueOf(evt.isContextIsAdmin()),
                String.valueOf(cTimestamp.withZoneRetainFields(DateTimeZone.UTC).getMillis()),
                evt.getPublisherId(),
                evt.getMessageId(),
                HiveFormatUtils.iterableToString(evt.getContextRoles(), HiveFormatUtils.COLLECTION_SEP_CHAR),
                String.valueOf(eTimestamp.withZoneRetainFields(DateTimeZone.UTC).getMillis()),
                String.valueOf(eTimestamp.getMonthOfYear()),
                String.valueOf(eTimestamp.getDayOfMonth()),
                String.valueOf(eTimestamp.getYear()),
                evt.getContextProjectName(),
                evt.getContextReadDeleted(),
                evt.getContextTenant(),
                String.valueOf(evt.isContextInstanceLockChecked()),
                evt.getContextProjectId(),
                evt.getContextUserName()
        );

        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull("").join(fields);
    }
}
