package com.rackspace.volga.etl.newrelic.dto.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id",
        "start",
        "end",
        "checks",
        "count"
})
public class Polls {

    @JsonProperty("id")
    private String id;
    
    @JsonProperty("start")
    private String start;
    
    @JsonProperty("end")
    private String end;
    
    @JsonProperty("checks")
    private List<Poll> polls = new ArrayList<Poll>();
    
    @JsonProperty("count")
    private long count;

    @JsonProperty("id")
	public String getId() {
		return id;
	}

    @JsonProperty("id")
	public void setId(String id) {
		this.id = id;
	}

    @JsonProperty("start")
	public String getStart() {
		return start;
	}

    @JsonProperty("start")
	public void setStart(String start) {
		this.start = start;
	}

    @JsonProperty("end")
	public String getEnd() {
		return end;
	}

    @JsonProperty("end")
	public void setEnd(String end) {
		this.end = end;
	}

    @JsonProperty("checks")
	public List<Poll> getPolls() {
		return polls;
	}

    @JsonProperty("checks")
	public void setPolls(Collection<Poll> polls) {
		this.polls.clear();
		this.polls.addAll(polls);
	}

    @JsonProperty("count")
	public long getCount() {
		return count;
	}

    @JsonProperty("count")
	public void setCount(long count) {
		this.count = count;
	}
}
