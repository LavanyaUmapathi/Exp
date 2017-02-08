package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.HashMap;
import java.util.Map;

public class SuperColumnWithSuperColumnRow {
	
	
	private Map<String, SuperColumnRow> superColumnWithSuperColumnRow = new HashMap<String, SuperColumnRow>();

	public SuperColumnWithSuperColumnRow() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Map<String, SuperColumnRow> getSuperColumnWithSuperColumnRow() {
		return superColumnWithSuperColumnRow;
	}

	public void setSuperColumnWithSuperColumnRow(
			Map<String, SuperColumnRow> superColumnWithSuperColumnRow) {
		this.superColumnWithSuperColumnRow = superColumnWithSuperColumnRow;
	}

}
