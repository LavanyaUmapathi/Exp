package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.annotation.Generated;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "primary",
        "address"
})
public class EmailAddress {

    @JsonProperty("primary")
    private boolean primary;

    @JsonProperty("address")
    private String address;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("primary")
    public boolean isPrimary() {
        return primary;
    }

    @JsonProperty("primary")
    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    public EmailAddress withPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    @JsonProperty("address")
    public String getAddress() {
        return address;
    }

    @JsonProperty("address")
    public void setAddress(String address) {
        this.address = address;
    }

    public EmailAddress withAddress(String address) {
        this.address = address;
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
