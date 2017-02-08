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
        "external_id",
        "metadata",
        "min_check_interval",
        "webhook_token",
        "soft_remove_ttl",
        "account_status",
        "rackspace_managed",
        "cep_group_id",
        "api_rate_limits",
        "limits",
        "features",
        "agent_bundle_channel",
        "check_type_channel",
        "metrics_ttl",
        "contacts",
        "entities"
})
public class Configuration {

    @JsonProperty("id")
    private String id;

    @JsonProperty("external_id")
    private String external_id;

    @JsonProperty("metadata")
    private Metadata metadata;

    @JsonProperty("min_check_interval")
    private long min_check_interval;

    @JsonProperty("webhook_token")
    private String webhook_token;

    @JsonProperty("soft_remove_ttl")
    private long soft_remove_ttl;

    @JsonProperty("account_status")
    private String account_status;

    @JsonProperty("rackspace_managed")
    private boolean rackspace_managed;

    @JsonProperty("cep_group_id")
    private String cep_group_id;

    @JsonProperty("api_rate_limits")
    private Map<String, Object> apiRateLimits;

    @JsonProperty("limits")

    private Limits limits;

    @JsonProperty("features")
    private Features features;

    @JsonProperty("agent_bundle_channel")
    private String agent_bundle_channel;

    @JsonProperty("check_type_channel")
    private String check_type_channel;

    @JsonProperty("metrics_ttl")
    private Map<String,Object> metricsTTL;

    @JsonProperty("contacts")
    private Contacts contacts;

    @JsonProperty("entities")
    private List<Entity> entities = new ArrayList<Entity>();

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    public Configuration withId(String id) {
        this.id = id;
        return this;
    }

    @JsonProperty("external_id")
    public String getExternal_id() {
        return external_id;
    }

    @JsonProperty("external_id")
    public void setExternal_id(String external_id) {
        this.external_id = external_id;
    }

    public Configuration withExternal_id(String external_id) {
        this.external_id = external_id;
        return this;
    }

    @JsonProperty("metadata")
    public Metadata getMetadata() {
        return metadata;
    }

    @JsonProperty("metadata")
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Configuration withMetadata(Metadata metadata) {
        this.metadata = metadata;
        return this;
    }

    @JsonProperty("min_check_interval")
    public long getMin_check_interval() {
        return min_check_interval;
    }

    @JsonProperty("min_check_interval")
    public void setMin_check_interval(long min_check_interval) {
        this.min_check_interval = min_check_interval;
    }

    public Configuration withMin_check_interval(long min_check_interval) {
        this.min_check_interval = min_check_interval;
        return this;
    }

    @JsonProperty("webhook_token")
    public String getWebhook_token() {
        return webhook_token;
    }

    @JsonProperty("webhook_token")
    public void setWebhook_token(String webhook_token) {
        this.webhook_token = webhook_token;
    }

    public Configuration withWebhook_token(String webhook_token) {
        this.webhook_token = webhook_token;
        return this;
    }

    @JsonProperty("soft_remove_ttl")
    public long getSoft_remove_ttl() {
        return soft_remove_ttl;
    }

    @JsonProperty("soft_remove_ttl")
    public void setSoft_remove_ttl(long soft_remove_ttl) {
        this.soft_remove_ttl = soft_remove_ttl;
    }

    public Configuration withSoft_remove_ttl(long soft_remove_ttl) {
        this.soft_remove_ttl = soft_remove_ttl;
        return this;
    }

    @JsonProperty("account_status")
    public String getAccount_status() {
        return account_status;
    }

    @JsonProperty("account_status")
    public void setAccount_status(String account_status) {
        this.account_status = account_status;
    }

    public Configuration withAccount_status(String account_status) {
        this.account_status = account_status;
        return this;
    }

    @JsonProperty("rackspace_managed")
    public boolean isRackspace_managed() {
        return rackspace_managed;
    }

    @JsonProperty("rackspace_managed")
    public void setRackspace_managed(boolean rackspace_managed) {
        this.rackspace_managed = rackspace_managed;
    }

    public Configuration withRackspace_managed(boolean rackspace_managed) {
        this.rackspace_managed = rackspace_managed;
        return this;
    }

    @JsonProperty("cep_group_id")
    public String getCep_group_id() {
        return cep_group_id;
    }

    @JsonProperty("cep_group_id")
    public void setCep_group_id(String cep_group_id) {
        this.cep_group_id = cep_group_id;
    }

    public Configuration withCep_group_id(String cep_group_id) {
        this.cep_group_id = cep_group_id;
        return this;
    }

    @JsonProperty("api_rate_limits")
    public Map<String, Object> getApiRateLimits() {
        return apiRateLimits;
    }

    @JsonProperty("api_rate_limits")
    public void setApiRateLimits(Map<String, Object> apiRateLimits) {
        this.apiRateLimits = apiRateLimits;
    }

    public Configuration withApiRateLimits(Map<String, Object> apiRateLimits) {
        this.apiRateLimits = apiRateLimits;
        return this;
    }

    @JsonProperty("limits")
    public Limits getLimits() {
        return limits;
    }

    @JsonProperty("limits")
    public void setLimits(Limits limits) {
        this.limits = limits;
    }

    public Configuration withLimits(Limits limits) {
        this.limits = limits;
        return this;
    }

    @JsonProperty("features")
    public Features getFeatures() {
        return features;
    }

    @JsonProperty("features")
    public void setFeatures(Features features) {
        this.features = features;
    }

    public Configuration withFeatures(Features features) {
        this.features = features;
        return this;
    }

    @JsonProperty("agent_bundle_channel")
    public String getAgent_bundle_channel() {
        return agent_bundle_channel;
    }

    @JsonProperty("agent_bundle_channel")
    public void setAgent_bundle_channel(String agent_bundle_channel) {
        this.agent_bundle_channel = agent_bundle_channel;
    }

    public Configuration withAgent_bundle_channel(String agent_bundle_channel) {
        this.agent_bundle_channel = agent_bundle_channel;
        return this;
    }

    @JsonProperty("check_type_channel")
    public String getCheck_type_channel() {
        return check_type_channel;
    }

    @JsonProperty("check_type_channel")
    public void setCheck_type_channel(String check_type_channel) {
        this.check_type_channel = check_type_channel;
    }

    public Configuration withCheck_type_channel(String check_type_channel) {
        this.check_type_channel = check_type_channel;
        return this;
    }

    @JsonProperty("metrics_ttl")
    public Map<String,Object> getMetricsTTL() {
        return metricsTTL;
    }

    @JsonProperty("metrics_ttl")
    public void setMetricsTTL(Map<String,Object> metricsTTL) {
        this.metricsTTL = metricsTTL;
    }

    public Configuration withMetricsTTL(Map<String,Object> metrics_ttl) {
        this.metricsTTL = metrics_ttl;
        return this;
    }

    @JsonProperty("contacts")
    public Contacts getContacts() {
        return contacts;
    }

    @JsonProperty("contacts")
    public void setContacts(Contacts contacts) {
        this.contacts = contacts;
    }

    public Configuration withContacts(Contacts contacts) {
        this.contacts = contacts;
        return this;
    }

    @JsonProperty("entities")
    public List<Entity> getEntities() {
        return entities;
    }

    @JsonProperty("entities")
    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    public Configuration withEntities(List<Entity> entities) {
        this.entities = entities;
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
