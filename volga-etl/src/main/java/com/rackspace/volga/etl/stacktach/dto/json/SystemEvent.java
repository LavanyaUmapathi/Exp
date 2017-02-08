package com.rackspace.volga.etl.stacktach.dto.json;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "_context_request_id",
        "_context_quota_class",
        "event_type",
        "_context_service_catalog",
        "_context_auth_token",
        "_context_user_id",
        "payload",
        "priority",
        "_context_is_admin",
        "_context_timestamp",
        "publisher_id",
        "message_id",
        "_context_remote_address",
        "_context_roles",
        "timestamp",
        "_context_user",
        "_unique_id",
        "_context_glance_api_servers",
        "_context_project_name",
        "_context_read_deleted",
        "_context_tenant",
        "_context_instance_lock_checked",
        "_context_project_id",
        "_context_user_name"
})
public class SystemEvent {

    @JsonProperty("_context_request_id")
    private String ContextRequestId;

    @JsonProperty("_context_quota_class")
    private Object ContextQuotaClass;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("_context_service_catalog")
    private List<Object> ContextServiceCatalog = Lists.newArrayList();

    @JsonProperty("_context_auth_token")
    private String ContextAuthToken;

    @JsonProperty("_context_user_id")
    private String ContextUserId;

    @JsonProperty("payload")
    private Payload payload;

    @JsonProperty("priority")
    private String priority;

    @JsonProperty("_context_is_admin")
    private boolean ContextIsAdmin;

    @JsonProperty("_context_timestamp")
    private String ContextTimestamp;

    @JsonProperty("publisher_id")
    private String publisherId;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("_context_remote_address")
    private String ContextRemoteAddress;

    @JsonProperty("_context_roles")
    private List<String> ContextRoles = Lists.newArrayList();

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("_context_user")
    private String ContextUser;

    @JsonProperty("_unique_id")
    private String UniqueId;

    @JsonProperty("_context_glance_api_servers")
    private Object ContextGlanceApiServers;

    @JsonProperty("_context_project_name")
    private String ContextProjectName;

    @JsonProperty("_context_read_deleted")
    private String ContextReadDeleted;

    @JsonProperty("_context_tenant")
    private String ContextTenant;

    @JsonProperty("_context_instance_lock_checked")
    private boolean ContextInstanceLockChecked;

    @JsonProperty("_context_project_id")
    private String ContextProjectId;

    @JsonProperty("_context_user_name")
    private String ContextUserName;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * @return The ContextRequestId
     */
    @JsonProperty("_context_request_id")
    public String getContextRequestId() {
        return ContextRequestId;
    }

    /**
     * @param ContextRequestId The _context_request_id
     */
    @JsonProperty("_context_request_id")
    public void setContextRequestId(String ContextRequestId) {
        this.ContextRequestId = ContextRequestId;
    }

    public SystemEvent withContextRequestId(String ContextRequestId) {
        this.ContextRequestId = ContextRequestId;
        return this;
    }

    /**
     * @return The ContextQuotaClass
     */
    @JsonProperty("_context_quota_class")
    public Object getContextQuotaClass() {
        return ContextQuotaClass;
    }

    /**
     * @param ContextQuotaClass The _context_quota_class
     */
    @JsonProperty("_context_quota_class")
    public void setContextQuotaClass(Object ContextQuotaClass) {
        this.ContextQuotaClass = ContextQuotaClass;
    }

    public SystemEvent withContextQuotaClass(Object ContextQuotaClass) {
        this.ContextQuotaClass = ContextQuotaClass;
        return this;
    }

    /**
     * @return The eventType
     */
    @JsonProperty("event_type")
    public String getEventType() {
        return eventType;
    }

    /**
     * @param eventType The event_type
     */
    @JsonProperty("event_type")
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public SystemEvent withEventType(String eventType) {
        this.eventType = eventType;
        return this;
    }

    /**
     * @return The ContextServiceCatalog
     */
    @JsonProperty("_context_service_catalog")
    public List<Object> getContextServiceCatalog() {
        return ContextServiceCatalog;
    }

    /**
     * @param ContextServiceCatalog The _context_service_catalog
     */
    @JsonProperty("_context_service_catalog")
    public void setContextServiceCatalog(List<Object> ContextServiceCatalog) {
        this.ContextServiceCatalog = ContextServiceCatalog;
    }

    public SystemEvent withContextServiceCatalog(List<Object> ContextServiceCatalog) {
        this.ContextServiceCatalog = ContextServiceCatalog;
        return this;
    }

    /**
     * @return The ContextAuthToken
     */
    @JsonProperty("_context_auth_token")
    public String getContextAuthToken() {
        return ContextAuthToken;
    }

    /**
     * @param ContextAuthToken The _context_auth_token
     */
    @JsonProperty("_context_auth_token")
    public void setContextAuthToken(String ContextAuthToken) {
        this.ContextAuthToken = ContextAuthToken;
    }

    public SystemEvent withContextAuthToken(String ContextAuthToken) {
        this.ContextAuthToken = ContextAuthToken;
        return this;
    }

    /**
     * @return The ContextUserId
     */
    @JsonProperty("_context_user_id")
    public String getContextUserId() {
        return ContextUserId;
    }

    /**
     * @param ContextUserId The _context_user_id
     */
    @JsonProperty("_context_user_id")
    public void setContextUserId(String ContextUserId) {
        this.ContextUserId = ContextUserId;
    }

    public SystemEvent withContextUserId(String ContextUserId) {
        this.ContextUserId = ContextUserId;
        return this;
    }

    /**
     * @return The payload
     */
    @JsonProperty("payload")
    public Payload getPayload() {
        return payload;
    }

    /**
     * @param payload The payload
     */
    @JsonProperty("payload")
    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public SystemEvent withPayload(Payload payload) {
        this.payload = payload;
        return this;
    }

    /**
     * @return The priority
     */
    @JsonProperty("priority")
    public String getPriority() {
        return priority;
    }

    /**
     * @param priority The priority
     */
    @JsonProperty("priority")
    public void setPriority(String priority) {
        this.priority = priority;
    }

    public SystemEvent withPriority(String priority) {
        this.priority = priority;
        return this;
    }

    /**
     * @return The ContextIsAdmin
     */
    @JsonProperty("_context_is_admin")
    public boolean isContextIsAdmin() {
        return ContextIsAdmin;
    }

    /**
     * @param ContextIsAdmin The _context_is_admin
     */
    @JsonProperty("_context_is_admin")
    public void setContextIsAdmin(boolean ContextIsAdmin) {
        this.ContextIsAdmin = ContextIsAdmin;
    }

    public SystemEvent withContextIsAdmin(boolean ContextIsAdmin) {
        this.ContextIsAdmin = ContextIsAdmin;
        return this;
    }

    /**
     * @return The ContextTimestamp
     */
    @JsonProperty("_context_timestamp")
    public String getContextTimestamp() {
        return ContextTimestamp;
    }

    /**
     * @param ContextTimestamp The _context_timestamp
     */
    @JsonProperty("_context_timestamp")
    public void setContextTimestamp(String ContextTimestamp) {
        this.ContextTimestamp = ContextTimestamp;
    }

    public SystemEvent withContextTimestamp(String ContextTimestamp) {
        this.ContextTimestamp = ContextTimestamp;
        return this;
    }

    /**
     * @return The publisherId
     */
    @JsonProperty("publisher_id")
    public String getPublisherId() {
        return publisherId;
    }

    /**
     * @param publisherId The publisher_id
     */
    @JsonProperty("publisher_id")
    public void setPublisherId(String publisherId) {
        this.publisherId = publisherId;
    }

    public SystemEvent withPublisherId(String publisherId) {
        this.publisherId = publisherId;
        return this;
    }

    /**
     * @return The messageId
     */
    @JsonProperty("message_id")
    public String getMessageId() {
        return messageId;
    }

    /**
     * @param messageId The message_id
     */
    @JsonProperty("message_id")
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public SystemEvent withMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    /**
     * @return The ContextRemoteAddress
     */
    @JsonProperty("_context_remote_address")
    public String getContextRemoteAddress() {
        return ContextRemoteAddress;
    }

    /**
     * @param ContextRemoteAddress The _context_remote_address
     */
    @JsonProperty("_context_remote_address")
    public void setContextRemoteAddress(String ContextRemoteAddress) {
        this.ContextRemoteAddress = ContextRemoteAddress;
    }

    public SystemEvent withContextRemoteAddress(String ContextRemoteAddress) {
        this.ContextRemoteAddress = ContextRemoteAddress;
        return this;
    }

    /**
     * @return The ContextRoles
     */
    @JsonProperty("_context_roles")
    public List<String> getContextRoles() {
        return ContextRoles;
    }

    /**
     * @param ContextRoles The _context_roles
     */
    @JsonProperty("_context_roles")
    public void setContextRoles(List<String> ContextRoles) {
        this.ContextRoles = ContextRoles;
    }

    public SystemEvent withContextRoles(List<String> ContextRoles) {
        this.ContextRoles = ContextRoles;
        return this;
    }

    /**
     * @return The timestamp
     */
    @JsonProperty("timestamp")
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp The timestamp
     */
    @JsonProperty("timestamp")
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public SystemEvent withTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * @return The ContextUser
     */
    @JsonProperty("_context_user")
    public String getContextUser() {
        return ContextUser;
    }

    /**
     * @param ContextUser The _context_user
     */
    @JsonProperty("_context_user")
    public void setContextUser(String ContextUser) {
        this.ContextUser = ContextUser;
    }

    public SystemEvent withContextUser(String ContextUser) {
        this.ContextUser = ContextUser;
        return this;
    }

    /**
     * @return The UniqueId
     */
    @JsonProperty("_unique_id")
    public String getUniqueId() {
        return UniqueId;
    }

    /**
     * @param UniqueId The _unique_id
     */
    @JsonProperty("_unique_id")
    public void setUniqueId(String UniqueId) {
        this.UniqueId = UniqueId;
    }

    public SystemEvent withUniqueId(String UniqueId) {
        this.UniqueId = UniqueId;
        return this;
    }

    /**
     * @return The ContextGlanceApiServers
     */
    @JsonProperty("_context_glance_api_servers")
    public Object getContextGlanceApiServers() {
        return ContextGlanceApiServers;
    }

    /**
     * @param ContextGlanceApiServers The _context_glance_api_servers
     */
    @JsonProperty("_context_glance_api_servers")
    public void setContextGlanceApiServers(Object ContextGlanceApiServers) {
        this.ContextGlanceApiServers = ContextGlanceApiServers;
    }

    public SystemEvent withContextGlanceApiServers(Object ContextGlanceApiServers) {
        this.ContextGlanceApiServers = ContextGlanceApiServers;
        return this;
    }

    /**
     * @return The ContextProjectName
     */
    @JsonProperty("_context_project_name")
    public String getContextProjectName() {
        return ContextProjectName;
    }

    /**
     * @param ContextProjectName The _context_project_name
     */
    @JsonProperty("_context_project_name")
    public void setContextProjectName(String ContextProjectName) {
        this.ContextProjectName = ContextProjectName;
    }

    public SystemEvent withContextProjectName(String ContextProjectName) {
        this.ContextProjectName = ContextProjectName;
        return this;
    }

    /**
     * @return The ContextReadDeleted
     */
    @JsonProperty("_context_read_deleted")
    public String getContextReadDeleted() {
        return ContextReadDeleted;
    }

    /**
     * @param ContextReadDeleted The _context_read_deleted
     */
    @JsonProperty("_context_read_deleted")
    public void setContextReadDeleted(String ContextReadDeleted) {
        this.ContextReadDeleted = ContextReadDeleted;
    }

    public SystemEvent withContextReadDeleted(String ContextReadDeleted) {
        this.ContextReadDeleted = ContextReadDeleted;
        return this;
    }

    /**
     * @return The ContextTenant
     */
    @JsonProperty("_context_tenant")
    public String getContextTenant() {
        return ContextTenant;
    }

    /**
     * @param ContextTenant The _context_tenant
     */
    @JsonProperty("_context_tenant")
    public void setContextTenant(String ContextTenant) {
        this.ContextTenant = ContextTenant;
    }

    public SystemEvent withContextTenant(String ContextTenant) {
        this.ContextTenant = ContextTenant;
        return this;
    }

    /**
     * @return The ContextInstanceLockChecked
     */
    @JsonProperty("_context_instance_lock_checked")
    public boolean isContextInstanceLockChecked() {
        return ContextInstanceLockChecked;
    }

    /**
     * @param ContextInstanceLockChecked The _context_instance_lock_checked
     */
    @JsonProperty("_context_instance_lock_checked")
    public void setContextInstanceLockChecked(boolean ContextInstanceLockChecked) {
        this.ContextInstanceLockChecked = ContextInstanceLockChecked;
    }

    public SystemEvent withContextInstanceLockChecked(boolean ContextInstanceLockChecked) {
        this.ContextInstanceLockChecked = ContextInstanceLockChecked;
        return this;
    }

    /**
     * @return The ContextProjectId
     */
    @JsonProperty("_context_project_id")
    public String getContextProjectId() {
        return ContextProjectId;
    }

    /**
     * @param ContextProjectId The _context_project_id
     */
    @JsonProperty("_context_project_id")
    public void setContextProjectId(String ContextProjectId) {
        this.ContextProjectId = ContextProjectId;
    }

    public SystemEvent withContextProjectId(String ContextProjectId) {
        this.ContextProjectId = ContextProjectId;
        return this;
    }

    /**
     * @return The ContextUserName
     */
    @JsonProperty("_context_user_name")
    public String getContextUserName() {
        return ContextUserName;
    }

    /**
     * @param ContextUserName The _context_user_name
     */
    @JsonProperty("_context_user_name")
    public void setContextUserName(String ContextUserName) {
        this.ContextUserName = ContextUserName;
    }

    public SystemEvent withContextUserName(String ContextUserName) {
        this.ContextUserName = ContextUserName;
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

    public SystemEvent withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(ContextRequestId).append(ContextQuotaClass).append(eventType).append
                (ContextServiceCatalog).append(ContextAuthToken).append(ContextUserId).append(payload).append
                (priority).append(ContextIsAdmin).append(ContextTimestamp).append(publisherId).append(messageId)
                .append(ContextRemoteAddress).append(ContextRoles).append(timestamp).append(ContextUser).append
                        (UniqueId).append(ContextGlanceApiServers).append(ContextProjectName).append
                        (ContextReadDeleted).append(ContextTenant).append(ContextInstanceLockChecked).append
                        (ContextProjectId).append(ContextUserName).append(additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof SystemEvent) == false) {
            return false;
        }
        SystemEvent rhs = ((SystemEvent) other);
        return new EqualsBuilder().append(ContextRequestId, rhs.ContextRequestId).append(ContextQuotaClass,
                rhs.ContextQuotaClass).append(eventType, rhs.eventType).append(ContextServiceCatalog,
                rhs.ContextServiceCatalog).append(ContextAuthToken, rhs.ContextAuthToken).append(ContextUserId,
                rhs.ContextUserId).append(payload, rhs.payload).append(priority, rhs.priority).append(ContextIsAdmin,
                rhs.ContextIsAdmin).append(ContextTimestamp, rhs.ContextTimestamp).append(publisherId,
                rhs.publisherId).append(messageId, rhs.messageId).append(ContextRemoteAddress,
                rhs.ContextRemoteAddress).append(ContextRoles, rhs.ContextRoles).append(timestamp,
                rhs.timestamp).append(ContextUser, rhs.ContextUser).append(UniqueId,
                rhs.UniqueId).append(ContextGlanceApiServers, rhs.ContextGlanceApiServers).append(ContextProjectName,
                rhs.ContextProjectName).append(ContextReadDeleted, rhs.ContextReadDeleted).append(ContextTenant,
                rhs.ContextTenant).append(ContextInstanceLockChecked, rhs.ContextInstanceLockChecked).append
                (ContextProjectId, rhs.ContextProjectId).append(ContextUserName,
                rhs.ContextUserName).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}