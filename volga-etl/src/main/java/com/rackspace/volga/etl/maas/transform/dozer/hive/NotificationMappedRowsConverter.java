package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.maas.dto.json.*;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 1:42 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class NotificationMappedRowsConverter extends MaasMappedRowsConverter<Notification> {

    public NotificationMappedRowsConverter() {
        super(MappedRows.class, Notification.class);
    }

    @Override
    public MappedRows convertFrom(Notification ntf, MappedRows rows) {
        String notificationString = getNotificationFields(ntf);
        if (rows == null) {
            rows = new MappedRows();
        }
        rows.addRow(NOTIFICATION_KEY, notificationString);
        String acctId = ntf.getEventId().split(":")[0];
        if (ntf.getEntity() != null) {
            rows.addRow(ENTITY_KEY, getEntityRow(ntf.getEntity(), null));
        }

        if (ntf.getCheck() != null) {
            List<String> checkFields = getCheckFields(ntf.getCheck(), ntf.getEntity(), acctId);
            checkFields.add(ntf.getEntity() != null ? ntf.getEntity().getId() : StringUtils.EMPTY);
            rows.addRow(CHECK_KEY, join(checkFields));
        }

        if (ntf.getAlarm() != null) {
            rows.addRow(ALARM_KEY, getAlarmRow(ntf.getAlarm()
                    , ntf.getEntity(), acctId));
        }

        return rows;
    }

    static String getNotificationFields(Notification ntf) {
        Alarm alarm = Optional.fromNullable(ntf.getAlarm()).or(new Alarm());
        Check check = Optional.fromNullable(ntf.getCheck()).or(new Check());
        Entity entity = Optional.fromNullable(ntf.getEntity()).or(new Entity());
        Details details = Optional.fromNullable(ntf.getDetails()).or(new Details());

        String entityId = entity.getId();
        String checkId = check.getId();
        String checkType = check.getType();
        String alarmId = alarm.getId();
        String target = HiveFormatUtils.sanitize(details.getTarget());
        String status = HiveFormatUtils.sanitize(details.getStatus());
        String state = HiveFormatUtils.sanitize(details.getState());
        DateTime date = new DateTime(details.getTimestamp(), DateTimeZone.UTC);
        List<String> fields = Lists.newArrayList(
                ntf.getEventId(),
                ntf.getLogEntryId(),
                ntf.getTenantId(),
                entityId,
                checkId,
                checkType,
                alarmId,
                target,
                String.valueOf(date.getMillis()),
                String.valueOf(date.getMonthOfYear()),
                String.valueOf(date.getDayOfMonth()),
                String.valueOf(date.getYear()),
                status,
                state);

        //Both 'l' and 'L' metric types will be saved as 'l' here because of how toStructMap works.
        //May need to revisit.
        if (details.getMetrics() != null) {
            fields.add(HiveFormatUtils.toStructMap(details.getMetrics(), new String[]{"type", "data", "unit"}));
        }

        fields.add(HiveFormatUtils.iterableToString(alarm.getActiveSuppressions(),
                HiveFormatUtils.COLLECTION_SEP_CHAR));
        fields.add(HiveFormatUtils.iterableToString(check.getActiveSuppressions(),
                HiveFormatUtils.COLLECTION_SEP_CHAR));
        fields.add(HiveFormatUtils.iterableToString(entity.getActiveSuppressions(),
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(Boolean.toString(ntf.isSurppressed()));
        fields.add(HiveFormatUtils.iterableToString(ntf.getNotifications(), HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.toStructList(ntf.getDetails().getObservations(),
                new String[]{"monitoring_zone_id", "state", "status", "timestamp", "collectorState"}));


        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(StringUtils.EMPTY).join(fields);
    }


}
