package com.rackspace.foundation.cassandra.service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.spring.HectorTemplateImpl;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;



@SuppressWarnings("serial")
public class ElasticCluster implements Serializable {

	private ThriftCluster thriftCluster;

	

	public ThriftCluster getThriftCluster() {
		return thriftCluster;
	}

	public void setThriftCluster(ThriftCluster thriftCluster) {
		this.thriftCluster = thriftCluster;
	}

	public Map<String, Map<String, ThriftColumnFamilyTemplate<String, String>>> getKeyspaceToStandardColumnFamily() {
		List<KeyspaceDefinition> keyspaceDefinitions = thriftCluster
				.describeKeyspaces();
		Map<String, Map<String, ThriftColumnFamilyTemplate<String, String>>> keyspaceToColumnFamily = new HashMap<String, Map<String, ThriftColumnFamilyTemplate<String, String>>>();
		for (KeyspaceDefinition kd : keyspaceDefinitions) {
			ExecutingKeyspace ek = new ExecutingKeyspace(kd.getName(),
					thriftCluster.getConnectionManager(),
					HFactory.createDefaultConsistencyLevelPolicy(),
					FailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
					thriftCluster.getCredentials());
			Map<String, ThriftColumnFamilyTemplate<String, String>> cfdsToTemplate = new HashMap<String, ThriftColumnFamilyTemplate<String, String>>();
			for (ColumnFamilyDefinition cfd : kd.getCfDefs()) {
				if (cfd.getColumnType().equals(ColumnType.STANDARD)) {
					cfdsToTemplate.put(cfd.getName(),
							new ThriftColumnFamilyTemplate<String, String>(ek,
									cfd.getName(), new StringSerializer(),
									new StringSerializer()));
				}
			}
			keyspaceToColumnFamily.put(kd.getName(), cfdsToTemplate);
		}
		return keyspaceToColumnFamily;
	}

	public Map<String, Map<String, ThriftSuperCfTemplate<String, String, String>>> getKeyspaceToSuperColumnFamily() {
		List<KeyspaceDefinition> keyspaceDefinitions = thriftCluster
				.describeKeyspaces();
		Map<String, Map<String, ThriftSuperCfTemplate<String, String, String>>> keyspaceToColumnFamily = new HashMap<String, Map<String, ThriftSuperCfTemplate<String, String, String>>>();

		for (KeyspaceDefinition kd : keyspaceDefinitions) {
			ExecutingKeyspace ek = new ExecutingKeyspace(kd.getName(),
					thriftCluster.getConnectionManager(),
					HFactory.createDefaultConsistencyLevelPolicy(),
					FailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
					thriftCluster.getCredentials());
			Map<String, ThriftSuperCfTemplate<String, String, String>> cfdsToTemplate = new HashMap<String, ThriftSuperCfTemplate<String, String, String>>();
			for (ColumnFamilyDefinition cfd : kd.getCfDefs()) {
				if (cfd.getColumnType().equals(ColumnType.SUPER)) {
					cfdsToTemplate.put(cfd.getName(),
							new ThriftSuperCfTemplate<String, String, String>(
									ek, cfd.getName(), new StringSerializer(),
									new StringSerializer(),
									new StringSerializer()));
				}
			}
			keyspaceToColumnFamily.put(kd.getName(), cfdsToTemplate);
		}
		return keyspaceToColumnFamily;
	}

	public String getClusterName() {
		return thriftCluster.getName();
	}

	public ThriftColumnFamilyTemplate<String, String> getStandardColumnFamilyTemplate(
			String keyspaceName, String columnFamilyName) {
		List<KeyspaceDefinition> keyspaceDefinitions = thriftCluster
				.describeKeyspaces();
		for (KeyspaceDefinition kd : keyspaceDefinitions) {
			if (kd.getName().equalsIgnoreCase(keyspaceName)) {
				ExecutingKeyspace ek = new ExecutingKeyspace(kd.getName(),
						thriftCluster.getConnectionManager(),
						HFactory.createDefaultConsistencyLevelPolicy(),
						FailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
						thriftCluster.getCredentials());
				for (ColumnFamilyDefinition cfd : kd.getCfDefs()) {
					if (cfd.getColumnType().equals(ColumnType.STANDARD)
							&& cfd.getName().equalsIgnoreCase(columnFamilyName)) {

						return new ThriftColumnFamilyTemplate<String, String>(
								ek, cfd.getName(), new StringSerializer(),
								new StringSerializer());
					}
				}
			}
		}
		return null;
	}

	public ThriftSuperCfTemplate<String, String, String> getSuperColumnFamilyTemplate(
			String keyspaceName, String columnFamilyName) {
		List<KeyspaceDefinition> keyspaceDefinitions = thriftCluster
				.describeKeyspaces();

		for (KeyspaceDefinition kd : keyspaceDefinitions) {
			ExecutingKeyspace ek = new ExecutingKeyspace(kd.getName(),
					thriftCluster.getConnectionManager(),
					HFactory.createDefaultConsistencyLevelPolicy(),
					FailoverPolicy.ON_FAIL_TRY_ONE_NEXT_AVAILABLE,
					thriftCluster.getCredentials());
			if (kd.getName().equalsIgnoreCase(keyspaceName)) {
				for (ColumnFamilyDefinition cfd : kd.getCfDefs()) {
					if (cfd.getColumnType().equals(ColumnType.SUPER)
							&& cfd.getName().equalsIgnoreCase(columnFamilyName)) {
						return new ThriftSuperCfTemplate<String, String, String>(
								ek, cfd.getName(), new StringSerializer(),
								new StringSerializer(), new StringSerializer());
					}
				}
			}
		}
		return null;
	}

	public HectorTemplate getHectorTemplate(String keyspaceName) {

		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		for (KeyspaceDefinition keyspace : keyspaceDefinitions) {
			if (keyspace.getName().equalsIgnoreCase(keyspaceName))
				return new HectorTemplateImpl(getThriftCluster(),
						keyspace.getName(), keyspace.getReplicationFactor(),
						keyspace.getStrategyClass(), null);
		}
		return null;
	}

	public Map<String, HectorTemplate> getHectorTemplates() {
		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();

		Map<String, HectorTemplate> templates = new HashMap<String, HectorTemplate>();
		for (KeyspaceDefinition keyspace : keyspaceDefinitions) {

			templates.put(
					keyspace.getName(),
					new HectorTemplateImpl(getThriftCluster(), keyspace
							.getName(), keyspace.getReplicationFactor(),
							keyspace.getStrategyClass(), null));
		}
		return templates;
	}

	public List<KeyspaceDefinition> getAllKeyspaceDefinition() {
		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		return keyspaceDefinitions;

	}

	/*
	 * Removes system keyspace from list
	 */
	public List<KeyspaceDefinition> getAllDataKeyspaceDefinition() {
		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		List<KeyspaceDefinition> dataKeyspaceDefintions = new ArrayList<KeyspaceDefinition>();
		for (KeyspaceDefinition keyspaceDefinition : keyspaceDefinitions) {
			if (!keyspaceDefinition.getName().equalsIgnoreCase("system")) {
				dataKeyspaceDefintions.add(keyspaceDefinition);
			}
		}
		return dataKeyspaceDefintions;

	}

	public KeyspaceDefinition getKeyspaceDefinition(String keyspaceName) {
		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		for (KeyspaceDefinition keyspaceDefinition : keyspaceDefinitions) {
			if (keyspaceDefinition.getName().equalsIgnoreCase(keyspaceName)) {
				return keyspaceDefinition;
			}
		}
		return null;
	}

	public List<ColumnFamilyDefinition> getAllColumnFamilyDefinitionsFromKeyspace(
			String keyspaceName) {

		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		for (KeyspaceDefinition keyspaceDefinition : keyspaceDefinitions) {
			if (keyspaceDefinition.getName().equalsIgnoreCase(keyspaceName)) {
				return keyspaceDefinition.getCfDefs();
			}
		}
		return null;
	}

	public ColumnFamilyDefinition getColumnFamilyDefinitionsFromKeyspace(
			String keyspaceName, String columnFamilyName) {

		List<KeyspaceDefinition> keyspaceDefinitions = getThriftCluster()
				.describeKeyspaces();
		for (KeyspaceDefinition keyspaceDefinition : keyspaceDefinitions) {
			if (keyspaceDefinition.getName().equalsIgnoreCase(keyspaceName)) {
				List<ColumnFamilyDefinition> columnFamilyDefinitions = keyspaceDefinition
						.getCfDefs();
				for (ColumnFamilyDefinition columnFamilyDefinition : columnFamilyDefinitions) {
					if (columnFamilyDefinition.getName().equalsIgnoreCase(
							columnFamilyName)) {
						return columnFamilyDefinition;
					}
				}
			}
		}
		return null;
	}

	
}
