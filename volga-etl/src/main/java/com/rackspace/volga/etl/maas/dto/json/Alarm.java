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
        "check_id",
        "criteria",
        "disabled",
        "notification_plan_id",
        "metadata",
        "created_at",
        "updated_at",
        "active_suppressions"

})
public class Alarm {

    @JsonProperty("id")
    private String id;

    @JsonProperty("label")
    private String label;

    @JsonProperty("check_id")
    private String checkId;

    @JsonProperty("criteria")
    private String criteria;

    @JsonProperty("disabled")
    private boolean disabled;

    @JsonProperty("notification_plan_id")
    private String notificationPlanId;

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

    public Alarm withId(String id) {
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

    public Alarm withLabel(String label) {
        this.label = label;
        return this;
    }

    @JsonProperty("check_id")
    public String getCheckId() {
        return checkId;
    }

    @JsonProperty("check_id")
    public void setCheckId(String checkId) {
        this.checkId = checkId;
    }


    @JsonProperty("criteria")
    public String getCriteria() {
        return criteria;
    }

    @JsonProperty("criteria")
    public void setCriteria(String criteria) {
        this.criteria = criteria;
    }

    public Alarm withCriteria(String criteria) {
        this.criteria = criteria;
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

    public Alarm withDisabled(boolean disabled) {
        this.disabled = disabled;
        return this;
    }

    @JsonProperty("notification_plan_id")
    public String getNotificationPlanId() {
        return notificationPlanId;
    }

    @JsonProperty("notification_plan_id")
    public void setNotificationPlanId(String notificationPlanId) {
        this.notificationPlanId = notificationPlanId;
    }


    @JsonProperty("metadata")
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @JsonProperty("metadata")
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public Alarm withMetadata(Map<String, Object> metadata) {
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

    public Alarm withCreated_at(long createdAt) {
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

    public Alarm withUpdated_at(long updatedAt) {
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
