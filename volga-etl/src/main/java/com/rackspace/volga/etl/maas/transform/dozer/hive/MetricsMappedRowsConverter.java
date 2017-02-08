package com.rackspace.volga.etl.maas.transform.dozer.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rackspace.volga.etl.common.data.MappedRows;
import com.rackspace.volga.etl.common.hive.HiveFormatUtils;
import com.rackspace.volga.etl.maas.dto.json.Metric;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;

/**
 * User: alex.silva
 * Date: 4/14/14
 * Time: 1:42 PM
 * Copyright Rackspace Hosting, Inc.
 */
public class MetricsMappedRowsConverter extends MaasMappedRowsConverter<Metric> {

    static final ImmutableMap<Integer, String> METRIC_TYPES = ImmutableMap.<Integer, String>builder()
            .put(MaasTypes.DOUBLE, "valueDbl")
            .put(MaasTypes.INT32, "valueI32")
            .put(MaasTypes.UINT32, "valueI32")
            .put(MaasTypes.INT64, "valueI64")
            .put(MaasTypes.UINT64, "valueI64")
            .put(MaasTypes.STRING, "valueStr")
            .put(MaasTypes.BOOLEAN, "valueBool")
            .build();

    public MetricsMappedRowsConverter() {
        super(MappedRows.class,Metric.class);
    }

    @Override
    public MappedRows convertFrom(Metric source, MappedRows rows) {
        if (rows == null) {
            rows = new MappedRows();
        }
        String metricString = getMetricFields(source);
        rows.addRow(METRIC_KEY, metricString);
        return rows;
    }


    private static String getMetricFields(Metric m) {
        DateTime date = new DateTime(m.getTimestamp()).withZone(DateTimeZone.UTC);
        List<String> baseFields = Lists.newArrayList(
                m.getId(),
                m.getAccountId(),
                m.getTenantId(),
                m.getEntityId(),
                m.getCheckId(),
                m.getDimensionKey(),
                HiveFormatUtils.sanitize(m.getTarget()),
                HiveFormatUtils.sanitize(m.getCheckType()),
                m.getMonitoringZoneId(),
                m.getCollectorId(),
                String.valueOf(m.isAvailable()),
                String.valueOf(date.getMillis()),
                String.valueOf(date.getMonthOfYear()),
                String.valueOf(date.getDayOfMonth()),
                String.valueOf(date.getYear())
        );

        baseFields.add(HiveFormatUtils.toStructMap(getMetricsMapFields(m.getMetrics())));
        return Joiner.on(HiveFormatUtils.FIELD_SEP_CHAR).useForNull(EMPTY).join(baseFields);
    }

    private static Map<String, List<String>> getMetricsMapFields(Map<String, Map<String, Object>> metrics) {
        Map<String, List<String>> values = Maps.newLinkedHashMap();
        if (metrics != null) {
            for (Map.Entry<String, Map<String, Object>> metric : metrics.entrySet()) {
                values.put(HiveFormatUtils.sanitize(metric.getKey()),
                        Lists.newArrayList(
                                HiveFormatUtils.sanitize(ObjectUtils.toString(metric.getValue().get("metricType"))),
                                getMetricValue(metric.getValue()),
                                HiveFormatUtils.sanitize(ObjectUtils.toString(metric.getValue().get("unitEnum"))),
                                HiveFormatUtils.sanitize(ObjectUtils.toString(metric.getValue().get("unitOtherStr"))))
                );
            }
        } else {
            values.put(EMPTY, Lists.newArrayList(EMPTY, EMPTY, EMPTY));
        }

        return values;
    }

    private static String getMetricValue(Map<String, Object> values) {
        String mappedValueType = METRIC_TYPES.get(values.get("metricType"));
        Object value = values.get(mappedValueType);
        return HiveFormatUtils.sanitize(value);
    }
}
