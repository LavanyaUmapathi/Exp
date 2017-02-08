package com.rackspace.foundation.cassandra.dao.query.impl;

import java.util.HashMap;
import java.util.Map;

public class ColumnResult {
	private Map<String,String> oneColumnResult=new HashMap<String,String>();

	public Map<String, String> getOneColumnResult() {
		return oneColumnResult;
	}

	public void setOneColumnResult(Map<String, String> oneColumnResult) {
		this.oneColumnResult = oneColumnResult;
	}

	public ColumnResult() {
		super();
	}

	public ColumnResult(Map<String, String> oneColumnResult) {
		super();
		this.oneColumnResult = oneColumnResult;
	}
	
}
