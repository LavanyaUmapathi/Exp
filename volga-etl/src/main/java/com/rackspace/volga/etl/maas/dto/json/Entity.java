package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.annotation.Generated;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "id",
        "label",
        "ip_addresses",
        "metadata",
        "managed",
        "uri",
        "agent_id",
        "created_at",
        "updated_at",
        "checks",
        "alarms"
})
public class Entity {

    @JsonProperty("id")
    private String id;

    @JsonProperty("label")
    private String label;

    @JsonProperty("ip_addresses")
    private Map<String, Object> ipAddresses;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;

    @JsonProperty("managed")
    private boolean managed;

    @JsonProperty("uri")
    private String uri;

    @JsonProperty("agent_id")
    private String agent_id;

    @JsonProperty("created_at")
    //Some created_ats were non numeric
    private String created_at;

    @JsonProperty("updated_at")
    private String updated_at;

    @JsonProperty("checks")
    private List<Check> checks = new ArrayList<Check>();

    @JsonProperty("alarms")
    private List<Alarm> alarms = new ArrayList<Alarm>();

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("active_suppressions")
    private List<String> activeSuppressions = new ArrayList<String>();

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    public Entity withId(String id) {
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

    public Entity withLabel(String label) {
        this.label = label;
        return this;
    }

    @JsonProperty("ip_addresses")
    public Map<String, Object> getIpAddresses() {
        return ipAddresses;
    }

    @JsonProperty("ip_addresses")
    public void setIpAddresses(Map<String, Object> ipAddresses) {
        this.ipAddresses = ipAddresses;
    }

    public Entity withIpAddresses(Map<String, Object> ipAddresses) {
        this.ipAddresses = ipAddresses;
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

    public Entity withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @JsonProperty("managed")
    public boolean isManaged() {
        return managed;
    }

    @JsonProperty("managed")
    public void setManaged(boolean managed) {
        this.managed = managed;
    }

    public Entity withManaged(boolean managed) {
        this.managed = managed;
        return this;
    }

    @JsonProperty("uri")
    public String getUri() {
        return uri;
    }

    @JsonProperty("uri")
    public void setUri(String uri) {
        this.uri = uri;
    }

    public Entity withUri(String uri) {
        this.uri = uri;
        return this;
    }

    @JsonProperty("agent_id")
    public String getAgent_id() {
        return agent_id;
    }

    @JsonProperty("agent_id")
    public void setAgent_id(String agent_id) {
        this.agent_id = agent_id;
    }

    public Entity withAgent_id(String agent_id) {
        this.agent_id = agent_id;
        return this;
    }

    @JsonProperty("created_at")
    public String getCreated_at() {
        return created_at;
    }

    @JsonProperty("created_at")
    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public Entity withCreated_at(String created_at) {
        this.created_at = created_at;
        return this;
    }

    @JsonProperty("updated_at")
    public String getUpdated_at() {
        return updated_at;
    }

    @JsonProperty("updated_at")
    public void setUpdated_at(String updated_at) {
        this.updated_at = updated_at;
    }

    public Entity withUpdated_at(String updated_at) {
        this.updated_at = updated_at;
        return this;
    }

    @JsonProperty("checks")
    public List<Check> getChecks() {
        return checks;
    }

    @JsonProperty("checks")
    public void setChecks(List<Check> checks) {
        this.checks = checks;
    }

    public Entity withChecks(List<Check> checks) {
        this.checks = checks;
        return this;
    }

    @JsonProperty("alarms")
    public List<Alarm> getAlarms() {
        return alarms;
    }

    @JsonProperty("alarms")
    public void setAlarms(List<Alarm> alarms) {
        this.alarms = alarms;
    }

    @JsonProperty("active_suppressions")
    public List<String> getActiveSuppressions() {
        return activeSuppressions;
    }

    @JsonProperty("active_suppressions")
    public void setActiveSuppressions(List<String> activeSuppressions) {
        this.activeSuppressions = activeSuppressions;
    }

    public Entity withAlarms(List<Alarm> alarms) {
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
