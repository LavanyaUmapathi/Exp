package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import com.rackspace.volga.etl.common.data.Timestampable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "event_id",
        "log_entry_id",
        "details",
        "entity",
        "check",
        "alarm",
        "tenant_id",
        "notifications",
        "isSuppressed"
})
public class Notification implements Timestampable {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("log_entry_id")
    private String logEntryId;

    @JsonProperty("details")
    private Details details;

    @JsonProperty("entity")
    private Entity entity;

    @JsonProperty("check")
    private Check check;

    @JsonProperty("alarm")
    private Alarm alarm;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("notifications")
    private List<String> notifications = new ArrayList<String>();

    @JsonProperty("isSuppressed")
    private boolean surppressed;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("event_id")
    public String getEventId() {
        return eventId;
    }

    @JsonProperty("event_id")
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    @JsonProperty("log_entry_id")
    public String getLogEntryId() {
        return logEntryId;
    }

    @JsonProperty("log_entry_id")
    public void setLogEntryId(String logEntryId) {
        this.logEntryId = logEntryId;
    }

    @JsonProperty("details")
    public Details getDetails() {
        return details;
    }

    @JsonProperty("details")
    public void setDetails(Details details) {
        this.details = details;
    }

    @JsonProperty("entity")
    public Entity getEntity() {
        return entity;
    }

    @JsonProperty("entity")
    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    @JsonProperty("check")
    public Check getCheck() {
        return check;
    }

    @JsonProperty("check")
    public void setCheck(Check check) {
        this.check = check;
    }

    @JsonProperty("alarm")
    public Alarm getAlarm() {
        return alarm;
    }

    @JsonProperty("alarm")
    public void setAlarm(Alarm alarm) {
        this.alarm = alarm;
    }

    @JsonProperty("tenant_id")
    public String getTenantId() {
        return tenantId;
    }

    @JsonProperty("tenant_id")
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @JsonProperty("isSuppressed")
    public boolean isSurppressed() {
        return surppressed;
    }

    @JsonProperty("isSuppressed")
    public void setSurppressed(boolean surppressed) {
        this.surppressed = surppressed;
    }

    @JsonProperty("notifications")
    public List<String> getNotifications() {
        return notifications;
    }

    @JsonProperty("notifications")
    public void setNotifications(List<String> notifications) {
        this.notifications = notifications;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public long getTimestamp() {
        return details != null ? details.getTimestamp() : 0;
    }

}
