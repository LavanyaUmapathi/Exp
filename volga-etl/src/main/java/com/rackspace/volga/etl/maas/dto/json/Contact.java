package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "userId",
        "firstName",
        "lastName",
        "emailAddresses",
        "emailAddress",
        "role",
        "roles",
        "link"
})
public class Contact {

    @JsonProperty("userId")
    private String userId;

    @JsonProperty("firstName")
    private String firstName;

    @JsonProperty("lastName")
    private String lastName;

    @JsonProperty("emailAddresses")
    private List<EmailAddress> emailAddresses = new ArrayList<EmailAddress>();

    @JsonProperty("emailAddress")
    private String emailAddress;

    @JsonProperty("role")
    private List<String> role = new ArrayList<String>();

    @JsonProperty("roles")
    private Roles roles;

    @JsonProperty("link")
    private String link;

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("userId")
    public String getUserId() {
        return userId;
    }

    @JsonProperty("userId")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Contact withUserId(String userId) {
        this.userId = userId;
        return this;
    }

    @JsonProperty("firstName")
    public String getFirstName() {
        return firstName;
    }

    @JsonProperty("firstName")
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public Contact withFirstName(String firstName) {
        this.firstName = firstName;
        return this;
    }

    @JsonProperty("lastName")
    public String getLastName() {
        return lastName;
    }

    @JsonProperty("lastName")
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Contact withLastName(String lastName) {
        this.lastName = lastName;
        return this;
    }

    @JsonProperty("emailAddresses")
    public List<EmailAddress> getEmailAddresses() {
        return emailAddresses;
    }

    @JsonProperty("emailAddresses")
    public void setEmailAddresses(List<EmailAddress> emailAddresses) {
        this.emailAddresses = emailAddresses;
    }

    public Contact withEmailAddresses(List<EmailAddress> emailAddresses) {
        this.emailAddresses = emailAddresses;
        return this;
    }

    @JsonProperty("emailAddress")
    public String getEmailAddress() {
        return emailAddress;
    }

    @JsonProperty("emailAddress")
    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public Contact withEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
        return this;
    }

    @JsonProperty("role")
    public List<String> getRole() {
        return role;
    }

    @JsonProperty("role")
    public void setRole(List<String> role) {
        this.role = role;
    }

    public Contact withRole(List<String> role) {
        this.role = role;
        return this;
    }

    @JsonProperty("roles")
    public Roles getRoles() {
        return roles;
    }

    @JsonProperty("roles")
    public void setRoles(Roles roles) {
        this.roles = roles;
    }

    public Contact withRoles(Roles roles) {
        this.roles = roles;
        return this;
    }

    @JsonProperty("link")
    public String getLink() {
        return link;
    }

    @JsonProperty("link")
    public void setLink(String link) {
        this.link = link;
    }

    public Contact withLink(String link) {
        this.link = link;
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
