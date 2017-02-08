package com.rackspace.volga.etl.newrelic.dto.json;

import com.fasterxml.jackson.annotation.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "time",
        "status"
})
public class Poll {
	
    @JsonProperty("time")
    private long time;
    
    @JsonProperty("status")
    private String status;

    public Poll() {
    	super();
    }
    
	public Poll(long time, String status) {
		setTime(time);
		setStatus(status);
	}

    @JsonProperty("time")
	public long getTime() {
		return time;
	}

    @JsonProperty("time")
	public void setTime(long time) {
    	//ensure timestamp contains millisecond precision
    	this.time = (time < 10000000000L) ? time * 1000 : time;
	}

    @JsonProperty("status")
	public String getStatus() {
		return status;
	}

    @JsonProperty("status")
	public void setStatus(String status) {
		this.status = status;
	}
}
