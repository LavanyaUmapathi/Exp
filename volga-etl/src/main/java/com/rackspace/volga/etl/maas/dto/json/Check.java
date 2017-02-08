package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id",
        "label",
        "type",
        "details",
        "monitoring_zones_poll",
        "timeout",
        "period",
        "target_alias",
        "target_hostname",
        "target_resolver",
        "disabled",
        "metadata",
        "created_at",
        "updated_at",
        "active_suppressions"
})
public class Check {

    @JsonProperty("id")
    private String id;

    @JsonProperty("label")
    private String label;

    @JsonProperty("type")
    private String type;

    @JsonProperty("details")
    private Map<String, Object> details;

    @JsonProperty("monitoring_zones_poll")
    private List<String> monitoring_zones_poll = new ArrayList<String>();

    @JsonProperty("timeout")
    private long timeout;

    @JsonProperty("period")
    private long period;

    @JsonProperty("target_alias")
    private String target_alias;

    @JsonProperty("target_hostname")
    private String target_hostname;

    @JsonProperty("target_resolver")
    private String target_resolver;

    @JsonProperty("disabled")
    private boolean disabled;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;

    @JsonProperty("created_at")
    private long createdAt;

    @JsonProperty("updated_at")
    private long updatedAt;

    @JsonProperty("active_suppressions")
    private List<String> activeSuppressions = new ArrayList<String>();

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    public Check withId(String id) {
        this.id = id;
        return this;
    }

    @JsonProperty("label")
    public String getLabel() {
        return label;
    }

    @JsonProperty("label")
    public void setLabel(String label) {
        this.label = label;
    }

    public Check withLabel(String label) {
        this.label = label;
        return this;
    }

    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    public Check withType(String type) {
        this.type = type;
        return this;
    }

    @JsonProperty("details")
    public Map<String, Object> getDetails() {
        return details;
    }

    @JsonProperty("details")
    public void setDetails(Map<String, Object> details) {
        this.details = details;
    }

    public Check withDetails(Map<String, Object> details) {
        this.details = details;
        return this;
    }

    @JsonProperty("monitoring_zones_poll")
    public List<String> getMonitoring_zones_poll() {
        return monitoring_zones_poll;
    }

    @JsonProperty("monitoring_zones_poll")
    public void setMonitoring_zones_poll(List<String> monitoring_zones_poll) {
        this.monitoring_zones_poll = monitoring_zones_poll;
    }

    public Check withMonitoring_zones_poll(List<String> monitoring_zones_poll) {
        this.monitoring_zones_poll = monitoring_zones_poll;
        return this;
    }

    @JsonProperty("timeout")
    public long getTimeout() {
        return timeout;
    }

    @JsonProperty("timeout")
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public Check withTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    @JsonProperty("period")
    public long getPeriod() {
        return period;
    }

    @JsonProperty("period")
    public void setPeriod(long period) {
        this.period = period;
    }

    public Check withPeriod(long period) {
        this.period = period;
        return this;
    }

    @JsonProperty("target_alias")
    public String getTarget_alias() {
        return target_alias;
    }

    @JsonProperty("target_alias")
    public void setTarget_alias(String target_alias) {
        this.target_alias = target_alias;
    }

    public Check withTarget_alias(String target_alias) {
        this.target_alias = target_alias;
        return this;
    }

    @JsonProperty("target_hostname")
    public String getTarget_hostname() {
        return target_hostname;
    }

    @JsonProperty("target_hostname")
    public void setTarget_hostname(String target_hostname) {
        this.target_hostname = target_hostname;
    }

    public Check withTarget_hostname(String target_hostname) {
        this.target_hostname = target_hostname;
        return this;
    }

    @JsonProperty("target_resolver")
    public String getTarget_resolver() {
        return target_resolver;
    }

    @JsonProperty("target_resolver")
    public void setTarget_resolver(String target_resolver) {
        this.target_resolver = target_resolver;
    }

    public Check withTarget_resolver(String target_resolver) {
        this.target_resolver = target_resolver;
        return this;
    }

    @JsonProperty("disabled")
    public boolean isDisabled() {
        return disabled;
    }

    @JsonProperty("disabled")
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public Check withDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    @JsonProperty("metadata")
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @JsonProperty("metadata")
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public Check withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @JsonProperty("created_at")
    public long getCreatedAt() {
        return createdAt;
    }

    @JsonProperty("created_at")
    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public Check withCreatedAt(long createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    @JsonProperty("updated_at")
    public long getUpdatedAt() {
        return updatedAt;
    }

    @JsonProperty("updated_at")
    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Check withUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
        return this;
    }

    @JsonProperty("active_suppressions")
    public List<String> getActiveSuppressions() {
        return activeSuppressions;
    }

    @JsonProperty("active_suppressions")
    public void setActiveSuppressions(List<String> activeSuppressions) {
        this.activeSuppressions = activeSuppressions;
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
