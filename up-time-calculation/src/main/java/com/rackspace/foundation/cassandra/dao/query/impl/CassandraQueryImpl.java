package com.rackspace.foundation.cassandra.dao.query.impl;

import java.io.Serializable;
import java.util.List;

import com.rackspace.foundation.cassandra.service.ElasticCluster;

public class CassandraQueryImpl implements Serializable{

	private static final long serialVersionUID = 1L;
	private ElasticCluster elasticCluster;
	private String keyspaceName;
	private String columnFamilyName;
	private List<String> keys;
	private List<String> columns;
	private String startSuperColumn;
	private String endSuperColumn;
	private boolean reverseResultOrder;
	private String superColumnName;
	private String key;
	private String startKey;
	private String endKey;
	private int rowCount = 1000;
	
	
	public ElasticCluster getElasticCluster() {
		return elasticCluster;
	}
	public void setElasticCluster(ElasticCluster elasticCluster) {
		this.elasticCluster = elasticCluster;
	}
	public String getKeyspaceName() {
		return keyspaceName;
	}
	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}
	public String getColumnFamilyName() {
		return columnFamilyName;
	}
	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
	public List<String> getKeys() {
		return keys;
	}
	public void setKeys(List<String> keys) {
		this.keys = keys;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public String getStartSuperColumn() {
		return startSuperColumn;
	}
	public void setStartSuperColumn(String startSuperColumn) {
		this.startSuperColumn = startSuperColumn;
	}
	public String getEndSuperColumn() {
		return endSuperColumn;
	}
	public void setEndSuperColumn(String endSuperColumn) {
		this.endSuperColumn = endSuperColumn;
	}
	public boolean isReverseResultOrder() {
		return reverseResultOrder;
	}
	public void setReverseResultOrder(boolean reverseResultOrder) {
		this.reverseResultOrder = reverseResultOrder;
	}
	public String getSuperColumnName() {
		return superColumnName;
	}
	public void setSuperColumnName(String superColumnName) {
		this.superColumnName = superColumnName;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getStartKey() {
		return startKey;
	}
	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}
	public String getEndKey() {
		return endKey;
	}
	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}
	public int getRowCount() {
		return rowCount;
	}
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
	
}
