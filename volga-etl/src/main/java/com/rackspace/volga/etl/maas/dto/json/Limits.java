package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.annotation.Generated;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "checks",
        "alarms"
})
public class Limits {

    @JsonProperty("checks")
    private long checks;

    @JsonProperty("alarms")
    private long alarms;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("checks")
    public long getChecks() {
        return checks;
    }

    @JsonProperty("checks")
    public void setChecks(long checks) {
        this.checks = checks;
    }

    public Limits withChecks(long checks) {
        this.checks = checks;
        return this;
    }

    @JsonProperty("alarms")
    public long getAlarms() {
        return alarms;
    }

    @JsonProperty("alarms")
    public void setAlarms(long alarms) {
        this.alarms = alarms;
    }

    public Limits withAlarms(long alarms) {
        this.alarms = alarms;
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
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
