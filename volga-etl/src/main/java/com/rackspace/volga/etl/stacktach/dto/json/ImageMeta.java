package com.rackspace.volga.etl.stacktach.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "com.rackspace__1__options",
        "container_format",
        "min_ram",
        "com.rackspace__1__build_rackconnect",
        "com.rackspace__1__build_core",
        "base_image_ref",
        "os_distro",
        "org.openstack__1__os_distro",
        "com.rackspace__1__release_id",
        "image_type",
        "com.rackspace__1__source",
        "disk_format",
        "com.rackspace__1__build_managed",
        "org.openstack__1__architecture",
        "com.rackspace__1__visible_core",
        "com.rackspace__1__release_build_date",
        "com.rackspace__1__visible_rackconnect",
        "min_disk",
        "com.rackspace__1__release_version",
        "com.rackspace__1__visible_managed",
        "cache_in_nova",
        "auto_disk_config",
        "os_type",
        "org.openstack__1__os_version"
})
public class ImageMeta {

    @JsonProperty("com.rackspace__1__options")
    private String comRackspace1Options;

    @JsonProperty("container_format")
    private String containerFormat;

    @JsonProperty("min_ram")
    private String minRam;

    @JsonProperty("com.rackspace__1__build_rackconnect")
    private String comRackspace1BuildRackconnect;

    @JsonProperty("com.rackspace__1__build_core")
    private String comRackspace1BuildCore;

    @JsonProperty("base_image_ref")
    private String baseImageRef;

    @JsonProperty("os_distro")
    private String osDistro;

    @JsonProperty("org.openstack__1__os_distro")
    private String orgOpenstack1OsDistro;

    @JsonProperty("com.rackspace__1__release_id")
    private String comRackspace1ReleaseId;

    @JsonProperty("image_type")
    private String imageType;

    @JsonProperty("com.rackspace__1__source")
    private String comRackspace1Source;

    @JsonProperty("disk_format")
    private String diskFormat;

    @JsonProperty("com.rackspace__1__build_managed")
    private String comRackspace1BuildManaged;

    @JsonProperty("org.openstack__1__architecture")
    private String orgOpenstack1Architecture;

    @JsonProperty("com.rackspace__1__visible_core")
    private String comRackspace1VisibleCore;

    @JsonProperty("com.rackspace__1__release_build_date")
    private String comRackspace1ReleaseBuildDate;

    @JsonProperty("com.rackspace__1__visible_rackconnect")
    private String comRackspace1VisibleRackconnect;

    @JsonProperty("min_disk")
    private String minDisk;

    @JsonProperty("com.rackspace__1__release_version")
    private String comRackspace1ReleaseVersion;

    @JsonProperty("com.rackspace__1__visible_managed")
    private String comRackspace1VisibleManaged;

    @JsonProperty("cache_in_nova")
    private String cacheInNova;

    @JsonProperty("auto_disk_config")
    private String autoDiskConfig;

    @JsonProperty("os_type")
    private String osType;

    @JsonProperty("org.openstack__1__os_version")
    private String orgOpenstack1OsVersion;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * @return The comRackspace1Options
     */
    @JsonProperty("com.rackspace__1__options")
    public String getComRackspace1Options() {
        return comRackspace1Options;
    }

    /**
     * @param comRackspace1Options The com.rackspace__1__options
     */
    @JsonProperty("com.rackspace__1__options")
    public void setComRackspace1Options(String comRackspace1Options) {
        this.comRackspace1Options = comRackspace1Options;
    }

    public ImageMeta withComRackspace1Options(String comRackspace1Options) {
        this.comRackspace1Options = comRackspace1Options;
        return this;
    }

    /**
     * @return The containerFormat
     */
    @JsonProperty("container_format")
    public String getContainerFormat() {
        return containerFormat;
    }

    /**
     * @param containerFormat The container_format
     */
    @JsonProperty("container_format")
    public void setContainerFormat(String containerFormat) {
        this.containerFormat = containerFormat;
    }

    public ImageMeta withContainerFormat(String containerFormat) {
        this.containerFormat = containerFormat;
        return this;
    }

    /**
     * @return The minRam
     */
    @JsonProperty("min_ram")
    public String getMinRam() {
        return minRam;
    }

    /**
     * @param minRam The min_ram
     */
    @JsonProperty("min_ram")
    public void setMinRam(String minRam) {
        this.minRam = minRam;
    }

    public ImageMeta withMinRam(String minRam) {
        this.minRam = minRam;
        return this;
    }

    /**
     * @return The comRackspace1BuildRackconnect
     */
    @JsonProperty("com.rackspace__1__build_rackconnect")
    public String getComRackspace1BuildRackconnect() {
        return comRackspace1BuildRackconnect;
    }

    /**
     * @param comRackspace1BuildRackconnect The com.rackspace__1__build_rackconnect
     */
    @JsonProperty("com.rackspace__1__build_rackconnect")
    public void setComRackspace1BuildRackconnect(String comRackspace1BuildRackconnect) {
        this.comRackspace1BuildRackconnect = comRackspace1BuildRackconnect;
    }

    public ImageMeta withComRackspace1BuildRackconnect(String comRackspace1BuildRackconnect) {
        this.comRackspace1BuildRackconnect = comRackspace1BuildRackconnect;
        return this;
    }

    /**
     * @return The comRackspace1BuildCore
     */
    @JsonProperty("com.rackspace__1__build_core")
    public String getComRackspace1BuildCore() {
        return comRackspace1BuildCore;
    }

    /**
     * @param comRackspace1BuildCore The com.rackspace__1__build_core
     */
    @JsonProperty("com.rackspace__1__build_core")
    public void setComRackspace1BuildCore(String comRackspace1BuildCore) {
        this.comRackspace1BuildCore = comRackspace1BuildCore;
    }

    public ImageMeta withComRackspace1BuildCore(String comRackspace1BuildCore) {
        this.comRackspace1BuildCore = comRackspace1BuildCore;
        return this;
    }

    /**
     * @return The baseImageRef
     */
    @JsonProperty("base_image_ref")
    public String getBaseImageRef() {
        return baseImageRef;
    }

    /**
     * @param baseImageRef The base_image_ref
     */
    @JsonProperty("base_image_ref")
    public void setBaseImageRef(String baseImageRef) {
        this.baseImageRef = baseImageRef;
    }

    public ImageMeta withBaseImageRef(String baseImageRef) {
        this.baseImageRef = baseImageRef;
        return this;
    }

    /**
     * @return The osDistro
     */
    @JsonProperty("os_distro")
    public String getOsDistro() {
        return osDistro;
    }

    /**
     * @param osDistro The os_distro
     */
    @JsonProperty("os_distro")
    public void setOsDistro(String osDistro) {
        this.osDistro = osDistro;
    }

    public ImageMeta withOsDistro(String osDistro) {
        this.osDistro = osDistro;
        return this;
    }

    /**
     * @return The orgOpenstack1OsDistro
     */
    @JsonProperty("org.openstack__1__os_distro")
    public String getOrgOpenstack1OsDistro() {
        return orgOpenstack1OsDistro;
    }

    /**
     * @param orgOpenstack1OsDistro The org.openstack__1__os_distro
     */
    @JsonProperty("org.openstack__1__os_distro")
    public void setOrgOpenstack1OsDistro(String orgOpenstack1OsDistro) {
        this.orgOpenstack1OsDistro = orgOpenstack1OsDistro;
    }

    public ImageMeta withOrgOpenstack1OsDistro(String orgOpenstack1OsDistro) {
        this.orgOpenstack1OsDistro = orgOpenstack1OsDistro;
        return this;
    }

    /**
     * @return The comRackspace1ReleaseId
     */
    @JsonProperty("com.rackspace__1__release_id")
    public String getComRackspace1ReleaseId() {
        return comRackspace1ReleaseId;
    }

    /**
     * @param comRackspace1ReleaseId The com.rackspace__1__release_id
     */
    @JsonProperty("com.rackspace__1__release_id")
    public void setComRackspace1ReleaseId(String comRackspace1ReleaseId) {
        this.comRackspace1ReleaseId = comRackspace1ReleaseId;
    }

    public ImageMeta withComRackspace1ReleaseId(String comRackspace1ReleaseId) {
        this.comRackspace1ReleaseId = comRackspace1ReleaseId;
        return this;
    }

    /**
     * @return The imageType
     */
    @JsonProperty("image_type")
    public String getImageType() {
        return imageType;
    }

    /**
     * @param imageType The image_type
     */
    @JsonProperty("image_type")
    public void setImageType(String imageType) {
        this.imageType = imageType;
    }

    public ImageMeta withImageType(String imageType) {
        this.imageType = imageType;
        return this;
    }

    /**
     * @return The comRackspace1Source
     */
    @JsonProperty("com.rackspace__1__source")
    public String getComRackspace1Source() {
        return comRackspace1Source;
    }

    /**
     * @param comRackspace1Source The com.rackspace__1__source
     */
    @JsonProperty("com.rackspace__1__source")
    public void setComRackspace1Source(String comRackspace1Source) {
        this.comRackspace1Source = comRackspace1Source;
    }

    public ImageMeta withComRackspace1Source(String comRackspace1Source) {
        this.comRackspace1Source = comRackspace1Source;
        return this;
    }

    /**
     * @return The diskFormat
     */
    @JsonProperty("disk_format")
    public String getDiskFormat() {
        return diskFormat;
    }

    /**
     * @param diskFormat The disk_format
     */
    @JsonProperty("disk_format")
    public void setDiskFormat(String diskFormat) {
        this.diskFormat = diskFormat;
    }

    public ImageMeta withDiskFormat(String diskFormat) {
        this.diskFormat = diskFormat;
        return this;
    }

    /**
     * @return The comRackspace1BuildManaged
     */
    @JsonProperty("com.rackspace__1__build_managed")
    public String getComRackspace1BuildManaged() {
        return comRackspace1BuildManaged;
    }

    /**
     * @param comRackspace1BuildManaged The com.rackspace__1__build_managed
     */
    @JsonProperty("com.rackspace__1__build_managed")
    public void setComRackspace1BuildManaged(String comRackspace1BuildManaged) {
        this.comRackspace1BuildManaged = comRackspace1BuildManaged;
    }

    public ImageMeta withComRackspace1BuildManaged(String comRackspace1BuildManaged) {
        this.comRackspace1BuildManaged = comRackspace1BuildManaged;
        return this;
    }

    /**
     * @return The orgOpenstack1Architecture
     */
    @JsonProperty("org.openstack__1__architecture")
    public String getOrgOpenstack1Architecture() {
        return orgOpenstack1Architecture;
    }

    /**
     * @param orgOpenstack1Architecture The org.openstack__1__architecture
     */
    @JsonProperty("org.openstack__1__architecture")
    public void setOrgOpenstack1Architecture(String orgOpenstack1Architecture) {
        this.orgOpenstack1Architecture = orgOpenstack1Architecture;
    }

    public ImageMeta withOrgOpenstack1Architecture(String orgOpenstack1Architecture) {
        this.orgOpenstack1Architecture = orgOpenstack1Architecture;
        return this;
    }

    /**
     * @return The comRackspace1VisibleCore
     */
    @JsonProperty("com.rackspace__1__visible_core")
    public String getComRackspace1VisibleCore() {
        return comRackspace1VisibleCore;
    }

    /**
     * @param comRackspace1VisibleCore The com.rackspace__1__visible_core
     */
    @JsonProperty("com.rackspace__1__visible_core")
    public void setComRackspace1VisibleCore(String comRackspace1VisibleCore) {
        this.comRackspace1VisibleCore = comRackspace1VisibleCore;
    }

    public ImageMeta withComRackspace1VisibleCore(String comRackspace1VisibleCore) {
        this.comRackspace1VisibleCore = comRackspace1VisibleCore;
        return this;
    }

    /**
     * @return The comRackspace1ReleaseBuildDate
     */
    @JsonProperty("com.rackspace__1__release_build_date")
    public String getComRackspace1ReleaseBuildDate() {
        return comRackspace1ReleaseBuildDate;
    }

    /**
     * @param comRackspace1ReleaseBuildDate The com.rackspace__1__release_build_date
     */
    @JsonProperty("com.rackspace__1__release_build_date")
    public void setComRackspace1ReleaseBuildDate(String comRackspace1ReleaseBuildDate) {
        this.comRackspace1ReleaseBuildDate = comRackspace1ReleaseBuildDate;
    }

    public ImageMeta withComRackspace1ReleaseBuildDate(String comRackspace1ReleaseBuildDate) {
        this.comRackspace1ReleaseBuildDate = comRackspace1ReleaseBuildDate;
        return this;
    }

    /**
     * @return The comRackspace1VisibleRackconnect
     */
    @JsonProperty("com.rackspace__1__visible_rackconnect")
    public String getComRackspace1VisibleRackconnect() {
        return comRackspace1VisibleRackconnect;
    }

    /**
     * @param comRackspace1VisibleRackconnect The com.rackspace__1__visible_rackconnect
     */
    @JsonProperty("com.rackspace__1__visible_rackconnect")
    public void setComRackspace1VisibleRackconnect(String comRackspace1VisibleRackconnect) {
        this.comRackspace1VisibleRackconnect = comRackspace1VisibleRackconnect;
    }

    public ImageMeta withComRackspace1VisibleRackconnect(String comRackspace1VisibleRackconnect) {
        this.comRackspace1VisibleRackconnect = comRackspace1VisibleRackconnect;
        return this;
    }

    /**
     * @return The minDisk
     */
    @JsonProperty("min_disk")
    public String getMinDisk() {
        return minDisk;
    }

    /**
     * @param minDisk The min_disk
     */
    @JsonProperty("min_disk")
    public void setMinDisk(String minDisk) {
        this.minDisk = minDisk;
    }

    public ImageMeta withMinDisk(String minDisk) {
        this.minDisk = minDisk;
        return this;
    }

    /**
     * @return The comRackspace1ReleaseVersion
     */
    @JsonProperty("com.rackspace__1__release_version")
    public String getComRackspace1ReleaseVersion() {
        return comRackspace1ReleaseVersion;
    }

    /**
     * @param comRackspace1ReleaseVersion The com.rackspace__1__release_version
     */
    @JsonProperty("com.rackspace__1__release_version")
    public void setComRackspace1ReleaseVersion(String comRackspace1ReleaseVersion) {
        this.comRackspace1ReleaseVersion = comRackspace1ReleaseVersion;
    }

    public ImageMeta withComRackspace1ReleaseVersion(String comRackspace1ReleaseVersion) {
        this.comRackspace1ReleaseVersion = comRackspace1ReleaseVersion;
        return this;
    }

    /**
     * @return The comRackspace1VisibleManaged
     */
    @JsonProperty("com.rackspace__1__visible_managed")
    public String getComRackspace1VisibleManaged() {
        return comRackspace1VisibleManaged;
    }

    /**
     * @param comRackspace1VisibleManaged The com.rackspace__1__visible_managed
     */
    @JsonProperty("com.rackspace__1__visible_managed")
    public void setComRackspace1VisibleManaged(String comRackspace1VisibleManaged) {
        this.comRackspace1VisibleManaged = comRackspace1VisibleManaged;
    }

    public ImageMeta withComRackspace1VisibleManaged(String comRackspace1VisibleManaged) {
        this.comRackspace1VisibleManaged = comRackspace1VisibleManaged;
        return this;
    }

    /**
     * @return The cacheInNova
     */
    @JsonProperty("cache_in_nova")
    public String getCacheInNova() {
        return cacheInNova;
    }

    /**
     * @param cacheInNova The cache_in_nova
     */
    @JsonProperty("cache_in_nova")
    public void setCacheInNova(String cacheInNova) {
        this.cacheInNova = cacheInNova;
    }

    public ImageMeta withCacheInNova(String cacheInNova) {
        this.cacheInNova = cacheInNova;
        return this;
    }

    /**
     * @return The autoDiskConfig
     */
    @JsonProperty("auto_disk_config")
    public String getAutoDiskConfig() {
        return autoDiskConfig;
    }

    /**
     * @param autoDiskConfig The auto_disk_config
     */
    @JsonProperty("auto_disk_config")
    public void setAutoDiskConfig(String autoDiskConfig) {
        this.autoDiskConfig = autoDiskConfig;
    }

    public ImageMeta withAutoDiskConfig(String autoDiskConfig) {
        this.autoDiskConfig = autoDiskConfig;
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

    public ImageMeta withOsType(String osType) {
        this.osType = osType;
        return this;
    }

    /**
     * @return The orgOpenstack1OsVersion
     */
    @JsonProperty("org.openstack__1__os_version")
    public String getOrgOpenstack1OsVersion() {
        return orgOpenstack1OsVersion;
    }

    /**
     * @param orgOpenstack1OsVersion The org.openstack__1__os_version
     */
    @JsonProperty("org.openstack__1__os_version")
    public void setOrgOpenstack1OsVersion(String orgOpenstack1OsVersion) {
        this.orgOpenstack1OsVersion = orgOpenstack1OsVersion;
    }

    public ImageMeta withOrgOpenstack1OsVersion(String orgOpenstack1OsVersion) {
        this.orgOpenstack1OsVersion = orgOpenstack1OsVersion;
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

    public ImageMeta withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(comRackspace1Options).append(containerFormat).append(minRam).append
                (comRackspace1BuildRackconnect).append(comRackspace1BuildCore).append(baseImageRef).append(osDistro)
                .append(orgOpenstack1OsDistro).append(comRackspace1ReleaseId).append(imageType).append
                        (comRackspace1Source).append(diskFormat).append(comRackspace1BuildManaged).append
                        (orgOpenstack1Architecture).append(comRackspace1VisibleCore).append
                        (comRackspace1ReleaseBuildDate).append(comRackspace1VisibleRackconnect).append(minDisk)
                .append(comRackspace1ReleaseVersion).append(comRackspace1VisibleManaged).append(cacheInNova).append
                        (autoDiskConfig).append(osType).append(orgOpenstack1OsVersion).append(additionalProperties)
                .toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ImageMeta) == false) {
            return false;
        }
        ImageMeta rhs = ((ImageMeta) other);
        return new EqualsBuilder().append(comRackspace1Options, rhs.comRackspace1Options).append(containerFormat,
                rhs.containerFormat).append(minRam, rhs.minRam).append(comRackspace1BuildRackconnect,
                rhs.comRackspace1BuildRackconnect).append(comRackspace1BuildCore, rhs.comRackspace1BuildCore).append
                (baseImageRef, rhs.baseImageRef).append(osDistro, rhs.osDistro).append(orgOpenstack1OsDistro,
                rhs.orgOpenstack1OsDistro).append(comRackspace1ReleaseId, rhs.comRackspace1ReleaseId).append
                (imageType, rhs.imageType).append(comRackspace1Source, rhs.comRackspace1Source).append(diskFormat,
                rhs.diskFormat).append(comRackspace1BuildManaged, rhs.comRackspace1BuildManaged).append
                (orgOpenstack1Architecture, rhs.orgOpenstack1Architecture).append(comRackspace1VisibleCore,
                rhs.comRackspace1VisibleCore).append(comRackspace1ReleaseBuildDate,
                rhs.comRackspace1ReleaseBuildDate).append(comRackspace1VisibleRackconnect,
                rhs.comRackspace1VisibleRackconnect).append(minDisk, rhs.minDisk).append(comRackspace1ReleaseVersion,
                rhs.comRackspace1ReleaseVersion).append(comRackspace1VisibleManaged,
                rhs.comRackspace1VisibleManaged).append(cacheInNova, rhs.cacheInNova).append(autoDiskConfig,
                rhs.autoDiskConfig).append(osType, rhs.osType).append(orgOpenstack1OsVersion,
                rhs.orgOpenstack1OsVersion).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}