package com.rackspace.foundation.cassandra.dao.query.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class RangeSliceQueryResult {
	private Map<String, Map<String,String>> queryResult= new LinkedHashMap<String,Map<String,String>>();

	public Map<String, Map<String,String>> getRangeSliceQueryResult() {
		return queryResult;
	}

	public void setRangeSliceQueryResult(
			Map<String, Map<String,String>> queryResult) {
		this.queryResult =queryResult;
	}

	

	public RangeSliceQueryResult() {
		super();
		// TODO Auto-generated constructor stub
	}

}
