package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.HashMap;
import java.util.Map;

public class SuperColumnRangeSliceQueryResult {
	private Map<String, SuperColumnWithSuperColumnRow> queryResult = new HashMap<String, SuperColumnWithSuperColumnRow>();

	public SuperColumnRangeSliceQueryResult() {
		super();
	}

	public Map<String, SuperColumnWithSuperColumnRow> getQueryResult() {
		return queryResult;
	}

	public void setQueryResult(
			Map<String, SuperColumnWithSuperColumnRow> queryResult) {
		this.queryResult = queryResult;
	}
	public boolean isEmpty(){
		if (queryResult.size()==0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}
