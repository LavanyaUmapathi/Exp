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
        "role"
})
public class Roles {

    @JsonProperty("role")
    private List<String> role = new ArrayList<String>();

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("role")
    public List<String> getRole() {
        return role;
    }

    @JsonProperty("role")
    public void setRole(List<String> role) {
        this.role = role;
    }

    public Roles withRole(List<String> role) {
        this.role = role;
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
