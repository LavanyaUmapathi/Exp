package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.HashMap;
import java.util.Map;

public class SuperColumnRow {
	private Map<String, String> rowColumns = new HashMap<String, String>();

	public SuperColumnRow() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Map<String, String> getRowColumns() {
		return rowColumns;
	}

	public void setRowColumns(Map<String, String> rowColumns) {
		this.rowColumns = rowColumns;
	}
}
