package com.rackspace.volga.etl.newrelic.dto.json;

import com.fasterxml.jackson.annotation.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id",
        "status",
        "name",
        "createdAt",
        "modifiedAt",
        "frequency",
        "location",
        "device",
        "account",
        "type",
        "friendlyName"
})
public class Monitor {
	
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("createdAt")
    private String createdAt;
    
    @JsonProperty("modifiedAt")
    private String modifiedAt;
    
    @JsonProperty("frequency")
    private long frequency;
    
    @JsonProperty("location")
    private String location;
    
    @JsonProperty("device")
    private String device;
    
    @JsonProperty("account")
    private String account;
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("friendlyName")
    private String friendlyName;

    @JsonProperty("id")
	public String getId() {
		return id;
	}

    @JsonProperty("id")
	public void setId(String id) {
		this.id = id;
	}

    @JsonProperty("status")
	public String getStatus() {
		return status;
	}

    @JsonProperty("status")
	public void setStatus(String status) {
		this.status = status;
	}

    @JsonProperty("name")
	public String getName() {
		return name;
	}

    @JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

    @JsonProperty("createdAt")
	public String getCreatedAt() {
		return createdAt;
	}

    @JsonProperty("createdAt")
	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}

    @JsonProperty("modifiedAt")
	public String getModifiedAt() {
		return modifiedAt;
	}

    @JsonProperty("modifiedAt")
	public void setModifiedAt(String modifiedAt) {
		this.modifiedAt = modifiedAt;
	}

    @JsonProperty("frequency")
	public long getFrequency() {
		return frequency;
	}

    @JsonProperty("frequency")
	public void setFrequency(long frequency) {
		this.frequency = frequency;
	}

    @JsonProperty("location")
	public String getLocation() {
		return location;
	}

    @JsonProperty("location")
	public void setLocation(String location) {
		this.location = location;
	}

    @JsonProperty("device")
	public String getDevice() {
		return device;
	}

    @JsonProperty("device")
	public void setDevice(String device) {
		this.device = device;
	}

    @JsonProperty("account")
	public String getAccount() {
		return account;
	}

    @JsonProperty("account")
	public void setAccount(String account) {
		this.account = account;
	}

    @JsonProperty("type")
	public String getType() {
		return type;
	}

    @JsonProperty("type")
	public void setType(String type) {
		this.type = type;
	}

    @JsonProperty("friendlyName")
	public String getFriendlyName() {
		return friendlyName;
	}

    @JsonProperty("friendlyName")
	public void setFriendlyName(String friendlyName) {
		this.friendlyName = friendlyName;
	}
}
