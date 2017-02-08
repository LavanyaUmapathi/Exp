package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.common.transform.BaseConverter;
import com.rackspace.volga.etl.maas.dto.json.*;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * User: alex.silva
 * Date: 7/24/14
 * Time: 12:19 PM
 * Copyright Rackspace Hosting, Inc.
 */
public abstract class MaasMappedRowsConverter<T> extends BaseConverter<MappedRows,T> {

    protected static final String CONFIG_KEY = "configuration";

    protected static final String CHECK_KEY = "check";

    protected static final String ENTITY_KEY = "entity";

    protected static final String ALARM_KEY = "alarm";

    protected static final String CONTACT_KEY = "contact";

    protected static final String NOTIFICATION_KEY = "notification";

    protected static final String METRIC_KEY = "metric";

    protected static final String EMPTY = StringUtils.EMPTY;

    protected MaasMappedRowsConverter(Class<MappedRows> prototypeA, Class<T> prototypeB) {
        super(prototypeA, prototypeB);
    }

    static List<String> getCheckFields(Check check, Entity entity, String acctId) {
        List<String> fields = Lists.newArrayList(
                HiveFormatUtils.sanitize(check.getId(), false),
                Optional.fromNullable(entity).or(new Entity()).getId(),
                acctId,
                HiveFormatUtils.sanitize(check.getLabel()),
                HiveFormatUtils.sanitize(check.getType()),
                String.valueOf(check.getTimeout()),
                String.valueOf(check.getPeriod()),
                HiveFormatUtils.sanitize(check.getTarget_alias()),
                HiveFormatUtils.sanitize(check.getTarget_hostname()),
                HiveFormatUtils.sanitize(check.getTarget_resolver()),
                String.valueOf(check.isDisabled()),
                String.valueOf(check.getCreatedAt()),
                String.valueOf(check.getUpdatedAt()));

        fields.add(HiveFormatUtils.mapToString(check.getDetails(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.iterableToString(check.getMonitoring_zones_poll(),
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.mapToString(check.getMetadata(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        return fields;
    }

    static String join(List<String> fields) {
        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(StringUtils.EMPTY).join(fields);
    }

    static String getCheckRow(Check check, Entity entity, String acctId) {
        return join(getCheckFields(check, entity, acctId));
    }

    static List<String> getAlarmFields(Alarm alarm, Entity entity, String acctId) {
        List<String> fields = Lists.newArrayList(
                alarm.getId(),
                StringUtils.lowerCase(alarm.getLabel()),
                Optional.fromNullable(entity).or(new Entity()).getId(),
                acctId,
                alarm.getCheckId(),
                String.valueOf(alarm.isDisabled()),
                alarm.getNotificationPlanId(),
                String.valueOf(alarm.getCreatedAt()),
                String.valueOf(alarm.getUpdatedAt())
        );
        fields.add(HiveFormatUtils.mapToString(alarm.getMetadata(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));
        return fields;
    }

    static String getAlarmRow(Alarm alarm, Entity entity, String acctId) {
        return join(getAlarmFields(alarm, entity, acctId));
    }


    static List<String> getEntityFields(Entity entity, Configuration config) {
        List<String> fields = Lists.newArrayList(entity.getId(),
                Optional.fromNullable(config).or(new Configuration()).getId(),
                HiveFormatUtils.sanitize(entity.getLabel()),
                String.valueOf(entity.isManaged()),
                HiveFormatUtils.sanitize(entity.getUri()),
                entity.getAgent_id(),
                String.valueOf(tryParseLong(entity.getCreated_at())),
                String.valueOf(tryParseLong(entity.getUpdated_at())));

        fields.add(HiveFormatUtils.mapToString(entity.getIpAddresses(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.mapToString(entity.getMetadata(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        return fields;

    }

    //Had to add this here because hive-exec is packaged with all google's Guava classes (not as a dependency.) Crazy.
    private static Long tryParseLong(String someText) {
        try {
            return Long.parseLong(someText);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    static String getEntityRow(Entity entity, Configuration config) {
        return join(getEntityFields(entity, config));
    }

    static String getConfigurationFields(Configuration cfg) {
        List<String> fields = Lists.newArrayList(
                cfg.getId(),
                cfg.getExternal_id(),
                String.valueOf(cfg.getMin_check_interval()),
                String.valueOf(cfg.getSoft_remove_ttl()),
                cfg.getAccount_status(),
                String.valueOf(cfg.isRackspace_managed()),
                cfg.getCep_group_id(),
                cfg.getAgent_bundle_channel(),
                cfg.getCheck_type_channel()
        );
        Contacts contacts = cfg.getContacts();
        fields.add(contacts != null ? HiveFormatUtils.getDistinctPropertyValues(contacts.getContacts(), "userId",
                HiveFormatUtils.COLLECTION_SEP_CHAR) : "");

        fields.add(HiveFormatUtils.getDistinctPropertyValues(cfg.getEntities(), "id",
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.mapToString(cfg.getApiRateLimits(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        fields.add(HiveFormatUtils.mapToString(cfg.getMetricsTTL(), HiveFormatUtils.MAP_SEP_CHAR,
                HiveFormatUtils.COLLECTION_SEP_CHAR));

        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull("").join(fields);
    }

    static String getContactFields(Contact contact, String date) {
      DateTime timestamp = new DateTime(date, DateTimeZone.UTC);
        List<String> fields = Lists.newArrayList(contact.getUserId(),
                HiveFormatUtils.sanitize(contact.getFirstName()),
                HiveFormatUtils.sanitize(contact.getLastName()),
                HiveFormatUtils.sanitize(contact.getEmailAddress()),
                HiveFormatUtils.sanitize(contact.getRole().get(0)),
                HiveFormatUtils.sanitize(contact.getLink()),
                String.valueOf(timestamp.getMillis()));

        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(StringUtils.EMPTY).join(fields);

    }
}
