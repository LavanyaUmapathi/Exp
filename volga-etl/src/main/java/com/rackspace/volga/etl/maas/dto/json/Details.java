package com.rackspace.volga.etl.maas.dto.json;

import com.fasterxml.jackson.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "target",
        "timestamp",
        "metrics",
        "state",
        "status",
        "txn_id",
        "observations"
})
public class Details {

    @JsonProperty("target")
    private String target;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("metrics")
    private Map<String,Map<String,Object>> metrics;

    @JsonProperty("state")
    private String state;

    @JsonProperty("status")
    private String status;

    @JsonProperty("txn_id")
    private String txnId;

    @JsonProperty("observations")
    private List<Observation> observations = new ArrayList<Observation>();

    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("target")
    public String getTarget() {
        return target;
    }

    @JsonProperty("target")
    public void setTarget(String target) {
        this.target = target;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("timestamp")
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("metrics")
    public Map<String,Map<String,Object>>  getMetrics() {
        return metrics;
    }

    @JsonProperty("metrics")
    public void setMetrics(Map<String,Map<String,Object>>  metrics) {
        this.metrics = metrics;
    }

    @JsonProperty("state")
    public String getState() {
        return state;
    }

    @JsonProperty("state")
    public void setState(String state) {
        this.state = state;
    }

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("txn_id")
    public String getTxnId() {
        return txnId;
    }

    @JsonProperty("txn_id")
    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    @JsonProperty("observations")
    public List<Observation> getObservations() {
        return observations;
    }

    @JsonProperty("observations")
    public void setObservations(List<Observation> observations) {
        this.observations = observations;
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
