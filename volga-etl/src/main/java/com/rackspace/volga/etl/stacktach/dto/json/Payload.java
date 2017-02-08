package com.rackspace.volga.etl.stacktach.dto.json;

import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "state_description",
        "availability_zone",
        "terminated_at",
        "ephemeral_gb",
        "instance_type_id",
        "bandwidth",
        "deleted_at",
        "reservation_id",
        "memory_mb",
        "display_name",
        "hostname",
        "state",
        "old_state",
        "progress",
        "launched_at",
        "metadata",
        "node",
        "ramdisk_id",
        "access_ip_v6",
        "disk_gb",
        "access_ip_v4",
        "kernel_id",
        "host",
        "user_id",
        "image_ref_url",
        "cell_name",
        "audit_period_beginning",
        "root_gb",
        "tenant_id",
        "created_at",
        "old_task_state",
        "instance_id",
        "instance_type",
        "vcpus",
        "image_meta",
        "architecture",
        "new_task_state",
        "audit_period_ending",
        "os_type",
        "instance_flavor_id"
})
public class Payload {

    @JsonProperty("state_description")
    private String stateDescription;

    @JsonProperty("availability_zone")
    private Object availabilityZone;

    @JsonProperty("terminated_at")
    private String terminatedAt;

    @JsonProperty("ephemeral_gb")
    private int ephemeralGb;

    @JsonProperty("instance_type_id")
    private int instanceTypeId;

    @JsonProperty("bandwidth")

    private Bandwidth bandwidth;

    @JsonProperty("deleted_at")
    private String deletedAt;

    @JsonProperty("reservation_id")
    private String reservationId;

    @JsonProperty("memory_mb")
    private int memoryMb;

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("state")
    private String state;

    @JsonProperty("old_state")
    private String oldState;

    @JsonProperty("progress")
    private String progress;

    @JsonProperty("launched_at")
    private String launchedAt;

    @JsonProperty("metadata")
    private Metadata metadata;

    @JsonProperty("node")
    private String node;

    @JsonProperty("ramdisk_id")
    private String ramdiskId;

    @JsonProperty("access_ip_v6")
    private String accessIpV6;

    @JsonProperty("disk_gb")
    private int diskGb;

    @JsonProperty("access_ip_v4")
    private String accessIpV4;

    @JsonProperty("kernel_id")
    private String kernelId;

    @JsonProperty("host")
    private String host;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("image_ref_url")
    private String imageRefUrl;

    @JsonProperty("cell_name")
    private String cellName;

    @JsonProperty("audit_period_beginning")
    private String auditPeriodBeginning;

    @JsonProperty("root_gb")
    private int rootGb;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("old_task_state")
    private String oldTaskState;

    @JsonProperty("instance_id")
    private String instanceId;

    @JsonProperty("instance_type")
    private String instanceType;

    @JsonProperty("vcpus")
    private int vcpus;

    @JsonProperty("image_meta")
    private ImageMeta imageMeta;

    @JsonProperty("architecture")
    private String architecture;

    @JsonProperty("new_task_state")
    private String newTaskState;

    @JsonProperty("audit_period_ending")
    private String auditPeriodEnding;

    @JsonProperty("os_type")
    private String osType;

    @JsonProperty("instance_flavor_id")
    private String instanceFlavorId;

    @JsonIgnore
    private Map<String, Object> additionalProperties = Maps.newHashMap();

    /**
     * @return The stateDescription
     */
    @JsonProperty("state_description")
    public String getStateDescription() {
        return stateDescription;
    }

    /**
     * @param stateDescription The state_description
     */
    @JsonProperty("state_description")
    public void setStateDescription(String stateDescription) {
        this.stateDescription = stateDescription;
    }

    public Payload withStateDescription(String stateDescription) {
        this.stateDescription = stateDescription;
        return this;
    }

    /**
     * @return The availabilityZone
     */
    @JsonProperty("availability_zone")
    public Object getAvailabilityZone() {
        return availabilityZone;
    }

    /**
     * @param availabilityZone The availability_zone
     */
    @JsonProperty("availability_zone")
    public void setAvailabilityZone(Object availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public Payload withAvailabilityZone(Object availabilityZone) {
        this.availabilityZone = availabilityZone;
        return this;
    }

    /**
     * @return The terminatedAt
     */
    @JsonProperty("terminated_at")
    public String getTerminatedAt() {
        return terminatedAt;
    }

    /**
     * @param terminatedAt The terminated_at
     */
    @JsonProperty("terminated_at")
    public void setTerminatedAt(String terminatedAt) {
        this.terminatedAt = terminatedAt;
    }

    public Payload withTerminatedAt(String terminatedAt) {
        this.terminatedAt = terminatedAt;
        return this;
    }

    /**
     * @return The ephemeralGb
     */
    @JsonProperty("ephemeral_gb")
    public int getEphemeralGb() {
        return ephemeralGb;
    }

    /**
     * @param ephemeralGb The ephemeral_gb
     */
    @JsonProperty("ephemeral_gb")
    public void setEphemeralGb(int ephemeralGb) {
        this.ephemeralGb = ephemeralGb;
    }

    public Payload withEphemeralGb(int ephemeralGb) {
        this.ephemeralGb = ephemeralGb;
        return this;
    }

    /**
     * @return The instanceTypeId
     */
    @JsonProperty("instance_type_id")
    public int getInstanceTypeId() {
        return instanceTypeId;
    }

    /**
     * @param instanceTypeId The instance_type_id
     */
    @JsonProperty("instance_type_id")
    public void setInstanceTypeId(int instanceTypeId) {
        this.instanceTypeId = instanceTypeId;
    }

    public Payload withInstanceTypeId(int instanceTypeId) {
        this.instanceTypeId = instanceTypeId;
        return this;
    }

    /**
     * @return The bandwidth
     */
    @JsonProperty("bandwidth")
    public Bandwidth getBandwidth() {
        return bandwidth;
    }

    /**
     * @param bandwidth The bandwidth
     */
    @JsonProperty("bandwidth")
    public void setBandwidth(Bandwidth bandwidth) {
        this.bandwidth = bandwidth;
    }

    public Payload withBandwidth(Bandwidth bandwidth) {
        this.bandwidth = bandwidth;
        return this;
    }

    /**
     * @return The deletedAt
     */
    @JsonProperty("deleted_at")
    public String getDeletedAt() {
        return deletedAt;
    }

    /**
     * @param deletedAt The deleted_at
     */
    @JsonProperty("deleted_at")
    public void setDeletedAt(String deletedAt) {
        this.deletedAt = deletedAt;
    }

    public Payload withDeletedAt(String deletedAt) {
        this.deletedAt = deletedAt;
        return this;
    }

    /**
     * @return The reservationId
     */
    @JsonProperty("reservation_id")
    public String getReservationId() {
        return reservationId;
    }

    /**
     * @param reservationId The reservation_id
     */
    @JsonProperty("reservation_id")
    public void setReservationId(String reservationId) {
        this.reservationId = reservationId;
    }

    public Payload withReservationId(String reservationId) {
        this.reservationId = reservationId;
        return this;
    }

    /**
     * @return The memoryMb
     */
    @JsonProperty("memory_mb")
    public int getMemoryMb() {
        return memoryMb;
    }

    /**
     * @param memoryMb The memory_mb
     */
    @JsonProperty("memory_mb")
    public void setMemoryMb(int memoryMb) {
        this.memoryMb = memoryMb;
    }

    public Payload withMemoryMb(int memoryMb) {
        this.memoryMb = memoryMb;
        return this;
    }

    /**
     * @return The displayName
     */
    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

    /**
     * @param displayName The display_name
     */
    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Payload withDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    /**
     * @return The hostname
     */
    @JsonProperty("hostname")
    public String getHostname() {
        return hostname;
    }

    /**
     * @param hostname The hostname
     */
    @JsonProperty("hostname")
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Payload withHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    /**
     * @return The state
     */
    @JsonProperty("state")
    public String getState() {
        return state;
    }

    /**
     * @param state The state
     */
    @JsonProperty("state")
    public void setState(String state) {
        this.state = state;
    }

    public Payload withState(String state) {
        this.state = state;
        return this;
    }

    /**
     * @return The oldState
     */
    @JsonProperty("old_state")
    public String getOldState() {
        return oldState;
    }

    /**
     * @param oldState The old_state
     */
    @JsonProperty("old_state")
    public void setOldState(String oldState) {
        this.oldState = oldState;
    }

    public Payload withOldState(String oldState) {
        this.oldState = oldState;
        return this;
    }

    /**
     * @return The progress
     */
    @JsonProperty("progress")
    public String getProgress() {
        return progress;
    }

    /**
     * @param progress The progress
     */
    @JsonProperty("progress")
    public void setProgress(String progress) {
        this.progress = progress;
    }

    public Payload withProgress(String progress) {
        this.progress = progress;
        return this;
    }

    /**
     * @return The launchedAt
     */
    @JsonProperty("launched_at")
    public String getLaunchedAt() {
        return launchedAt;
    }

    /**
     * @param launchedAt The launched_at
     */
    @JsonProperty("launched_at")
    public void setLaunchedAt(String launchedAt) {
        this.launchedAt = launchedAt;
    }

    public Payload withLaunchedAt(String launchedAt) {
        this.launchedAt = launchedAt;
        return this;
    }

    /**
     * @return The metadata
     */
    @JsonProperty("metadata")
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * @param metadata The metadata
     */
    @JsonProperty("metadata")
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Payload withMetadata(Metadata metadata) {
        this.metadata = metadata;
        return this;
    }

    /**
     * @return The node
     */
    @JsonProperty("node")
    public String getNode() {
        return node;
    }

    /**
     * @param node The node
     */
    @JsonProperty("node")
    public void setNode(String node) {
        this.node = node;
    }

    public Payload withNode(String node) {
        this.node = node;
        return this;
    }

    /**
     * @return The ramdiskId
     */
    @JsonProperty("ramdisk_id")
    public String getRamdiskId() {
        return ramdiskId;
    }

    /**
     * @param ramdiskId The ramdisk_id
     */
    @JsonProperty("ramdisk_id")
    public void setRamdiskId(String ramdiskId) {
        this.ramdiskId = ramdiskId;
    }

    public Payload withRamdiskId(String ramdiskId) {
        this.ramdiskId = ramdiskId;
        return this;
    }

    /**
     * @return The accessIpV6
     */
    @JsonProperty("access_ip_v6")
    public String getAccessIpV6() {
        return accessIpV6;
    }

    /**
     * @param accessIpV6 The access_ip_v6
     */
    @JsonProperty("access_ip_v6")
    public void setAccessIpV6(String accessIpV6) {
        this.accessIpV6 = accessIpV6;
    }

    public Payload withAccessIpV6(String accessIpV6) {
        this.accessIpV6 = accessIpV6;
        return this;
    }

    /**
     * @return The diskGb
     */
    @JsonProperty("disk_gb")
    public int getDiskGb() {
        return diskGb;
    }

    /**
     * @param diskGb The disk_gb
     */
    @JsonProperty("disk_gb")
    public void setDiskGb(int diskGb) {
        this.diskGb = diskGb;
    }

    public Payload withDiskGb(int diskGb) {
        this.diskGb = diskGb;
        return this;
    }

    /**
     * @return The accessIpV4
     */
    @JsonProperty("access_ip_v4")
    public String getAccessIpV4() {
        return accessIpV4;
    }

    /**
     * @param accessIpV4 The access_ip_v4
     */
    @JsonProperty("access_ip_v4")
    public void setAccessIpV4(String accessIpV4) {
        this.accessIpV4 = accessIpV4;
    }

    public Payload withAccessIpV4(String accessIpV4) {
        this.accessIpV4 = accessIpV4;
        return this;
    }

    /**
     * @return The kernelId
     */
    @JsonProperty("kernel_id")
    public String getKernelId() {
        return kernelId;
    }

    /**
     * @param kernelId The kernel_id
     */
    @JsonProperty("kernel_id")
    public void setKernelId(String kernelId) {
        this.kernelId = kernelId;
    }

    public Payload withKernelId(String kernelId) {
        this.kernelId = kernelId;
        return this;
    }

    /**
     * @return The host
     */
    @JsonProperty("host")
    public String getHost() {
        return host;
    }

    /**
     * @param host The host
     */
    @JsonProperty("host")
    public void setHost(String host) {
        this.host = host;
    }

    public Payload withHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * @return The userId
     */
    @JsonProperty("user_id")
    public String getUserId() {
        return userId;
    }

    /**
     * @param userId The user_id
     */
    @JsonProperty("user_id")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Payload withUserId(String userId) {
        this.userId = userId;
        return this;
    }

    /**
     * @return The imageRefUrl
     */
    @JsonProperty("image_ref_url")
    public String getImageRefUrl() {
        return imageRefUrl;
    }

    /**
     * @param imageRefUrl The image_ref_url
     */
    @JsonProperty("image_ref_url")
    public void setImageRefUrl(String imageRefUrl) {
        this.imageRefUrl = imageRefUrl;
    }

    public Payload withImageRefUrl(String imageRefUrl) {
        this.imageRefUrl = imageRefUrl;
        return this;
    }

    /**
     * @return The cellName
     */
    @JsonProperty("cell_name")
    public String getCellName() {
        return cellName;
    }

    /**
     * @param cellName The cell_name
     */
    @JsonProperty("cell_name")
    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public Payload withCellName(String cellName) {
        this.cellName = cellName;
        return this;
    }

    /**
     * @return The auditPeriodBeginning
     */
    @JsonProperty("audit_period_beginning")
    public String getAuditPeriodBeginning() {
        return auditPeriodBeginning;
    }

    /**
     * @param auditPeriodBeginning The audit_period_beginning
     */
    @JsonProperty("audit_period_beginning")
    public void setAuditPeriodBeginning(String auditPeriodBeginning) {
        this.auditPeriodBeginning = auditPeriodBeginning;
    }

    public Payload withAuditPeriodBeginning(String auditPeriodBeginning) {
        this.auditPeriodBeginning = auditPeriodBeginning;
        return this;
    }

    /**
     * @return The rootGb
     */
    @JsonProperty("root_gb")
    public int getRootGb() {
        return rootGb;
    }

    /**
     * @param rootGb The root_gb
     */
    @JsonProperty("root_gb")
    public void setRootGb(int rootGb) {
        this.rootGb = rootGb;
    }

    public Payload withRootGb(int rootGb) {
        this.rootGb = rootGb;
        return this;
    }

    /**
     * @return The tenantId
     */
    @JsonProperty("tenant_id")
    public String getTenantId() {
        return tenantId;
    }

    /**
     * @param tenantId The tenant_id
     */
    @JsonProperty("tenant_id")
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Payload withTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    /**
     * @return The createdAt
     */
    @JsonProperty("created_at")
    public String getCreatedAt() {
        return createdAt;
    }

    /**
     * @param createdAt The created_at
     */
    @JsonProperty("created_at")
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public Payload withCreatedAt(String createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    /**
     * @return The oldTaskState
     */
    @JsonProperty("old_task_state")
    public String getOldTaskState() {
        return oldTaskState;
    }

    /**
     * @param oldTaskState The old_task_state
     */
    @JsonProperty("old_task_state")
    public void setOldTaskState(String oldTaskState) {
        this.oldTaskState = oldTaskState;
    }

    public Payload withOldTaskState(String oldTaskState) {
        this.oldTaskState = oldTaskState;
        return this;
    }

    /**
     * @return The instanceId
     */
    @JsonProperty("instance_id")
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * @param instanceId The instance_id
     */
    @JsonProperty("instance_id")
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Payload withInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    /**
     * @return The instanceType
     */
    @JsonProperty("instance_type")
    public String getInstanceType() {
        return instanceType;
    }

    /**
     * @param instanceType The instance_type
     */
    @JsonProperty("instance_type")
    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public Payload withInstanceType(String instanceType) {
        this.instanceType = instanceType;
        return this;
    }

    /**
     * @return The vcpus
     */
    @JsonProperty("vcpus")
    public int getVcpus() {
        return vcpus;
    }

    /**
     * @param vcpus The vcpus
     */
    @JsonProperty("vcpus")
    public void setVcpus(int vcpus) {
        this.vcpus = vcpus;
    }

    public Payload withVcpus(int vcpus) {
        this.vcpus = vcpus;
        return this;
    }

    /**
     * @return The imageMeta
     */
    @JsonProperty("image_meta")
    public ImageMeta getImageMeta() {
        return imageMeta;
    }

    /**
     * @param imageMeta The image_meta
     */
    @JsonProperty("image_meta")
    public void setImageMeta(ImageMeta imageMeta) {
        this.imageMeta = imageMeta;
    }

    public Payload withImageMeta(ImageMeta imageMeta) {
        this.imageMeta = imageMeta;
        return this;
    }

    /**
     * @return The architecture
     */
    @JsonProperty("architecture")
    public String getArchitecture() {
        return architecture;
    }

    /**
     * @param architecture The architecture
     */
    @JsonProperty("architecture")
    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    public Payload withArchitecture(String architecture) {
        this.architecture = architecture;
        return this;
    }

    /**
     * @return The newTaskState
     */
    @JsonProperty("new_task_state")
    public String getNewTaskState() {
        return newTaskState;
    }

    /**
     * @param newTaskState The new_task_state
     */
    @JsonProperty("new_task_state")
    public void setNewTaskState(String newTaskState) {
        this.newTaskState = newTaskState;
    }

    public Payload withNewTaskState(String newTaskState) {
        this.newTaskState = newTaskState;
        return this;
    }

    /**
     * @return The auditPeriodEnding
     */
    @JsonProperty("audit_period_ending")
    public String getAuditPeriodEnding() {
        return auditPeriodEnding;
    }

    /**
     * @param auditPeriodEnding The audit_period_ending
     */
    @JsonProperty("audit_period_ending")
    public void setAuditPeriodEnding(String auditPeriodEnding) {
        this.auditPeriodEnding = auditPeriodEnding;
    }

    public Payload withAuditPeriodEnding(String auditPeriodEnding) {
        this.auditPeriodEnding = auditPeriodEnding;
        return this;
    }

    /**
     * @return The osType
     */
    @JsonProperty("os_type")
    public String getOsType() {
        return osType;
    }

    /**
     * @param osType The os_type
     */
    @JsonProperty("os_type")
    public void setOsType(String osType) {
        this.osType = osType;
    }

    public Payload withOsType(String osType) {
        this.osType = osType;
        return this;
    }

    /**
     * @return The instanceFlavorId
     */
    @JsonProperty("instance_flavor_id")
    public String getInstanceFlavorId() {
        return instanceFlavorId;
    }

    /**
     * @param instanceFlavorId The instance_flavor_id
     */
    @JsonProperty("instance_flavor_id")
    public void setInstanceFlavorId(String instanceFlavorId) {
        this.instanceFlavorId = instanceFlavorId;
    }

    public Payload withInstanceFlavorId(String instanceFlavorId) {
        this.instanceFlavorId = instanceFlavorId;
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

    public Payload withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(stateDescription).append(availabilityZone).append(terminatedAt).append
                (ephemeralGb).append(instanceTypeId).append(bandwidth).append(deletedAt).append(reservationId).append
                (memoryMb).append(displayName).append(hostname).append(state).append(oldState).append(progress)
                .append(launchedAt).append(metadata).append(node).append(ramdiskId).append(accessIpV6).append(diskGb)
                .append(accessIpV4).append(kernelId).append(host).append(userId).append(imageRefUrl).append(cellName)
                .append(auditPeriodBeginning).append(rootGb).append(tenantId).append(createdAt).append(oldTaskState)
                .append(instanceId).append(instanceType).append(vcpus).append(imageMeta).append(architecture).append
                        (newTaskState).append(auditPeriodEnding).append(osType).append(instanceFlavorId).append
                        (additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Payload) == false) {
            return false;
        }
        Payload rhs = ((Payload) other);
        return new EqualsBuilder().append(stateDescription, rhs.stateDescription).append(availabilityZone,
                rhs.availabilityZone).append(terminatedAt, rhs.terminatedAt).append(ephemeralGb,
                rhs.ephemeralGb).append(instanceTypeId, rhs.instanceTypeId).append(bandwidth,
                rhs.bandwidth).append(deletedAt, rhs.deletedAt).append(reservationId,
                rhs.reservationId).append(memoryMb, rhs.memoryMb).append(displayName,
                rhs.displayName).append(hostname, rhs.hostname).append(state, rhs.state).append(oldState,
                rhs.oldState).append(progress, rhs.progress).append(launchedAt, rhs.launchedAt).append(metadata,
                rhs.metadata).append(node, rhs.node).append(ramdiskId, rhs.ramdiskId).append(accessIpV6,
                rhs.accessIpV6).append(diskGb, rhs.diskGb).append(accessIpV4, rhs.accessIpV4).append(kernelId,
                rhs.kernelId).append(host, rhs.host).append(userId, rhs.userId).append(imageRefUrl,
                rhs.imageRefUrl).append(cellName, rhs.cellName).append(auditPeriodBeginning,
                rhs.auditPeriodBeginning).append(rootGb, rhs.rootGb).append(tenantId, rhs.tenantId).append(createdAt,
                rhs.createdAt).append(oldTaskState, rhs.oldTaskState).append(instanceId,
                rhs.instanceId).append(instanceType, rhs.instanceType).append(vcpus, rhs.vcpus).append(imageMeta,
                rhs.imageMeta).append(architecture, rhs.architecture).append(newTaskState,
                rhs.newTaskState).append(auditPeriodEnding, rhs.auditPeriodEnding).append(osType,
                rhs.osType).append(instanceFlavorId, rhs.instanceFlavorId).append(additionalProperties,
                rhs.additionalProperties).isEquals();
    }

}