package com.rackspace.foundation.cassandra.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.spring.HectorTemplateImpl;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import  com.rackspace.foundation.cassandra.service.ElasticCluster;
public class ClusterBuilder {

	public static Map<String, Map<String, ThriftColumnFamilyTemplate<String, String>>> getKeyspaceDefinition(
			ThriftCluster thriftCluster, Serializer<String> key,
			Serializer<String> top) {
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
									cfd.getName(), key, top));
				}
			}
			keyspaceToColumnFamily.put(kd.getName(), cfdsToTemplate);
		}
		return keyspaceToColumnFamily;
	}

	// Might need to change for specifics
	public static Map<String, Map<String, ThriftSuperCfTemplate<String, String, String>>> getKeyspaceDefinitionToSuperColumnFamily(
			ThriftCluster thriftCluster, Serializer<String> key,
			Serializer<String> top, Serializer<String> sub) {
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
									ek, cfd.getName(), key, top, sub));
				}
			}
			keyspaceToColumnFamily.put(kd.getName(), cfdsToTemplate);
		}
		return keyspaceToColumnFamily;
	}

	public static Map<String, HectorTemplate> getHectorTemplates(
			ThriftCluster cluster) {
		List<KeyspaceDefinition> keyspaceDefinitions = cluster
				.describeKeyspaces();

		Map<String, HectorTemplate> templates = new HashMap<String, HectorTemplate>();
		for (KeyspaceDefinition keyspace : keyspaceDefinitions) {

			templates.put(keyspace.getName(), new HectorTemplateImpl(cluster,
					keyspace.getName(), keyspace.getReplicationFactor(),
					keyspace.getStrategyClass(), null));
		}
		return templates;

	}

	public static ElasticCluster getCluster(String clusterName, String hosts) {

		CassandraHostConfigurator config = new CassandraHostConfigurator(hosts);
		config.setAutoDiscoverHosts(true);
		// config.setAutoDiscoveryDelayInSeconds(5);
		// TO DO: Bug in Hector API; Do not set setRunAutoDiscoveryAtStartup to
		// true for now
		config.setRunAutoDiscoveryAtStartup(false);
		config.setLoadBalancingPolicy(new DynamicLoadBalancingPolicy());

		return getCluster(clusterName, config);

	}

	public static ElasticCluster getCluster(String clusterName,
			CassandraHostConfigurator config) {

		ElasticCluster elasticCluster = new ElasticCluster();
		ThriftCluster cluster = ((ThriftCluster) HFactory.getOrCreateCluster(
				clusterName, config));
		elasticCluster.setThriftCluster(cluster);
		return elasticCluster;
	}

	// Bug in thrift api and when auto discovery and getCluster method is used.

	/*
	 * public static ElasticCluster getCluster(String clusterName,
	 * CassandraHostConfigurator config, Map<String, String> credentials) {
	 * 
	 * ElasticCluster elasticCluster = new ElasticCluster(); ThriftCluster
	 * cluster = ((ThriftCluster) HFactory.createCluster( clusterName, config,
	 * credentials)); elasticCluster.setThriftCluster(cluster);
	 * elasticCluster.setHectorTemplates(getHectorTemplates(cluster));
	 * elasticCluster.setKeyspaceToColumnFamily(getKeyspaceDefinition(cluster,
	 * new StringSerializer(), new StringSerializer())); return elasticCluster;
	 * }
	 */

}
