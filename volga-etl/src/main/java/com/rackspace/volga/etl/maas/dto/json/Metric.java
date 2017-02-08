package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import com.rackspace.volga.etl.common.data.Timestampable;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "version",
        "id",
        "timestamp",
        "accountId",
        "tenantId",
        "entityId",
        "checkId",
        "dimensionKey",
        "target",
        "checkType",
        "monitoringZoneId",
        "collectorId",
        "available",
        "metrics"
})
public class Metric  implements Timestampable {

    @JsonProperty("version")
    private long version;

    @JsonProperty("id")
    private String id;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("accountId")
    private String accountId;

    @JsonProperty("tenantId")
    private String tenantId;

    @JsonProperty("entityId")
    private String entityId;

    @JsonProperty("checkId")
    private String checkId;

    @JsonProperty("dimensionKey")
    private String dimensionKey;

    @JsonProperty("target")
    private String target;

    @JsonProperty("checkType")
    private String checkType;

    @JsonProperty("monitoringZoneId")
    private String monitoringZoneId;

    @JsonProperty("collectorId")
    private String collectorId;

    @JsonProperty("available")
    private boolean available;

    @JsonProperty("metrics")
    private Map<String,Map<String,Object>> metrics;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("version")
    public long getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(long version) {
        this.version = version;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("accountId")
    public String getAccountId() {
        return accountId;
    }

    @JsonProperty("accountId")
    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    @JsonProperty("tenantId")
    public String getTenantId() {
        return tenantId;
    }

    @JsonProperty("tenantId")
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @JsonProperty("entityId")
    public String getEntityId() {
        return entityId;
    }

    @JsonProperty("entityId")
    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    @JsonProperty("checkId")
    public String getCheckId() {
        return checkId;
    }

    @JsonProperty("checkId")
    public void setCheckId(String checkId) {
        this.checkId = checkId;
    }

    @JsonProperty("dimensionKey")
    public String getDimensionKey() {
        return dimensionKey;
    }

    @JsonProperty("dimensionKey")
    public void setDimensionKey(String dimensionKey) {
        this.dimensionKey = dimensionKey;
    }

    @JsonProperty("target")
    public String getTarget() {
        return target;
    }

    @JsonProperty("target")
    public void setTarget(String target) {
        this.target = target;
    }

    @JsonProperty("checkType")
    public String getCheckType() {
        return checkType;
    }

    @JsonProperty("checkType")
    public void setCheckType(String checkType) {
        this.checkType = checkType;
    }

    @JsonProperty("monitoringZoneId")
    public String getMonitoringZoneId() {
        return monitoringZoneId;
    }

    @JsonProperty("monitoringZoneId")
    public void setMonitoringZoneId(String monitoringZoneId) {
        this.monitoringZoneId = monitoringZoneId;
    }

    @JsonProperty("collectorId")
    public String getCollectorId() {
        return collectorId;
    }

    @JsonProperty("collectorId")
    public void setCollectorId(String collectorId) {
        this.collectorId = collectorId;
    }

    @JsonProperty("available")
    public boolean isAvailable() {
        return available;
    }

    @JsonProperty("available")
    public void setAvailable(boolean available) {
        this.available = available;
    }

    @JsonProperty("metrics")
    public Map<String,Map<String,Object>> getMetrics() {
        return metrics;
    }

    @JsonProperty("metrics")
    public void setMetrics(Map<String,Map<String,Object>> metrics) {
        this.metrics = metrics;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
