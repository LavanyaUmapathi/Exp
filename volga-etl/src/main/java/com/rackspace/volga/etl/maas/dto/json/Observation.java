package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "monitoring_zone_id",
        "state",
        "status",
        "timestamp",
        "collectorState"
})
public class Observation {

    @JsonProperty("monitoring_zone_id")
    private Object monitoring_zone_id;

    @JsonProperty("state")
    private String state;

    @JsonProperty("status")
    private String status;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("collectorState")
    private String collectorState;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("monitoring_zone_id")
    public Object getMonitoring_zone_id() {
        return monitoring_zone_id;
    }

    @JsonProperty("monitoring_zone_id")
    public void setMonitoring_zone_id(Object monitoring_zone_id) {
        this.monitoring_zone_id = monitoring_zone_id;
    }

    @JsonProperty("state")
    public String getState() {
        return state;
    }

    @JsonProperty("state")
    public void setState(String state) {
        this.state = state;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("collectorState")
    public String getCollectorState() {
        return collectorState;
    }

    @JsonProperty("collectorState")
    public void setCollectorState(String collectorState) {
        this.collectorState = collectorState;
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
