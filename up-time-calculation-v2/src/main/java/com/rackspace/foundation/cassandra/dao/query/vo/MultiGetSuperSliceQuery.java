package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import org.apache.log4j.Logger;
//import org.springframework.stereotype.Component;

import com.rackspace.foundation.cassandra.service.ElasticCluster;

public class MultiGetSuperSliceQuery {
	
	private static org.apache.log4j.Logger log = Logger
			.getLogger(SuperColumnRangeSliceQuery.class);
	
	private ElasticCluster elasticCluster;
	private String keyspaceName;
	private String columnFamilyName;
	private List<String> keys;
	private List<String> superColumns;
	private String startSuperColumn;
	private String endSuperColumn;
	private boolean reverseResultOrder;
	
	
	public List<Map<String, Map<String,Map<String,String>>>> executeQuery_getList()
	{
		log.warn("In Multi Get SuperSlice Query - keys size"+ keys.size());
		List<Map<String, Map<String,Map<String,String>>>> result= new ArrayList<Map<String, Map<String,Map<String,String>>>>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		
		MultigetSuperSliceQuery<String,String, String,String> query=hTemplate.createMultigetSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	
		query.setColumnFamily( this.columnFamilyName);
		
		
		if (superColumns != null)
		{
			
			query.setColumnNames(superColumns.get(0));
		}
		else
		{
			query.setRange(this.startSuperColumn, this.endSuperColumn, this.reverseResultOrder, 100000);
		}
		

		
		query.setKeys(keys);
		log.warn("Executing query");
		
		QueryResult<SuperRows<String,String,String,String>> queryResult=query.execute();
		
		log.warn("Query Executed");
		
		String currentRowKey="";
		String currentSuperColumn="";
		for (SuperRow<String,String,String,String> oneSuperRow: queryResult.get() )
		{
			if (oneSuperRow.getSuperSlice().getSuperColumns().size()>0)
			{
				Map<String, Map<String,Map<String,String>>> oneRowSuperColumns = new LinkedHashMap<String, Map<String,Map<String,String>>>();

				currentRowKey=oneSuperRow.getKey();
				Map<String,Map<String,String>> oneSuperColumn = new LinkedHashMap<String, Map<String,String>>();
				for (HSuperColumn<String,String,String> oneSuperColumnResult: oneSuperRow.getSuperSlice().getSuperColumns())
				{
					currentSuperColumn= oneSuperColumnResult.getName();
					Map<String,String> oneSuperColumnRowAttribute = new LinkedHashMap<String,String>();
					for (HColumn<String,String> oneColumn : oneSuperColumnResult.getColumns())
					{
						oneSuperColumnRowAttribute.put(oneColumn.getName(), oneColumn.getValue());
					}
					oneSuperColumn.put(currentSuperColumn, oneSuperColumnRowAttribute);
					
				}
				
				oneRowSuperColumns.put(currentRowKey,  oneSuperColumn);
				//oneSuperColumn.clear();
				result.add(oneRowSuperColumns);
				//oneRowSuperColumns.clear();
			}
		}
				
	//	}		

		hTemplate=null;
		query=null;
		return result;
	}
	public Map<String, Map<String,Map<String,String>>> executeQuery()
	{
		log.warn("In Multi Get SuperSlice Query - keys size"+ keys.size());
		//List<Map<String, Map<String,Map<String,String>>>> result= new ArrayList<Map<String, Map<String,Map<String,String>>>>();
		Map<String, Map<String,Map<String,String>>> oneResult = new LinkedHashMap<String, Map<String,Map<String,String>>>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		
		MultigetSuperSliceQuery<String,String, String,String> query=hTemplate.createMultigetSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	
		query.setColumnFamily( this.columnFamilyName);
		
		
		if (superColumns != null)
		{
			
			query.setColumnNames(superColumns.get(0));
		}
		else
		{
			query.setRange(this.startSuperColumn, this.endSuperColumn, this.reverseResultOrder, 100000);
		}
		

		
		query.setKeys(keys);
		log.warn("Executing query");
		
		QueryResult<SuperRows<String,String,String,String>> queryResult=query.execute();
		
		log.warn("Query Executed");
		
		String currentRowKey="";
		String currentSuperColumn="";
		for (SuperRow<String,String,String,String> oneSuperRow: queryResult.get() )
		{
			if (oneSuperRow.getSuperSlice().getSuperColumns().size()>0)
			{

				currentRowKey=oneSuperRow.getKey();
				Map<String,Map<String,String>> oneSuperColumn = new LinkedHashMap<String, Map<String,String>>();
				for (HSuperColumn<String,String,String> oneSuperColumnResult: oneSuperRow.getSuperSlice().getSuperColumns())
				{
					currentSuperColumn= oneSuperColumnResult.getName();
					Map<String,String> oneSuperColumnRowAttribute = new LinkedHashMap<String,String>();
					for (HColumn<String,String> oneColumn : oneSuperColumnResult.getColumns())
					{
						oneSuperColumnRowAttribute.put(oneColumn.getName(), oneColumn.getValue());
					}
					oneSuperColumn.put(currentSuperColumn, oneSuperColumnRowAttribute);
					
				}
				
				
				oneResult.put(currentRowKey,  oneSuperColumn);

			}
		}
				

		
		hTemplate=null;
		query=null;
		return oneResult;
	}
	
	public List<Map<String, Map<String,Map<String,String>>>> getPreviousSuperColumn_getList()
	{
		List<Map<String, Map<String,Map<String,String>>>> result= new ArrayList<Map<String, Map<String,Map<String,String>>>>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		
		MultigetSuperSliceQuery<String,String, String,String> query=hTemplate.createMultigetSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	
		query.setColumnFamily( this.columnFamilyName);
		
		
	
		query.setRange(this.startSuperColumn, "0", true,1);
		
		

		
		query.setKeys(keys);
		
		log.warn("Geting previous values forom column family:"+this.columnFamilyName+" and number of keys:"+keys.size());
		
		QueryResult<SuperRows<String,String,String,String>> queryResult=query.execute();
		log.warn("Got previous values ");
		String currentRowKey="";
		String currentSuperColumn="";
		for (SuperRow<String,String,String,String> oneSuperRow: queryResult.get() )
		{
			if (oneSuperRow.getSuperSlice().getSuperColumns().size()>0)
			{
				Map<String, Map<String,Map<String,String>>> oneRowSuperColumns = new LinkedHashMap<String, Map<String,Map<String,String>>>();

				currentRowKey=oneSuperRow.getKey();
				Map<String,Map<String,String>> oneSuperColumn = new LinkedHashMap<String, Map<String,String>>();
				for (HSuperColumn<String,String,String> oneSuperColumnResult: oneSuperRow.getSuperSlice().getSuperColumns())
				{
					currentSuperColumn= oneSuperColumnResult.getName();
					Map<String,String> oneSuperColumnRowAttribute = new LinkedHashMap<String,String>();
					for (HColumn<String,String> oneColumn : oneSuperColumnResult.getColumns())
					{
						oneSuperColumnRowAttribute.put(oneColumn.getName(), oneColumn.getValue());
					}
					oneSuperColumn.put(currentSuperColumn, oneSuperColumnRowAttribute);
					
				}
				
				oneRowSuperColumns.put(currentRowKey,  oneSuperColumn);

				result.add(oneRowSuperColumns);

			}
		}
				
				
	
		hTemplate=null;
		query=null;
		return result;
	}
	
	public Map<String, Map<String,Map<String,String>>> getPreviousSuperColumn()
	{
		Map<String, Map<String,Map<String,String>>> oneResult = new LinkedHashMap<String, Map<String,Map<String,String>>>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		
		MultigetSuperSliceQuery<String,String, String,String> query=hTemplate.createMultigetSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
	
		query.setColumnFamily( this.columnFamilyName);
		
		
	
		query.setRange(this.startSuperColumn, "0", true,1);
		
		

		
		query.setKeys(keys);
		
		log.warn("Geting previous values forom column family:"+this.columnFamilyName+" and number of keys:"+keys.size());
		
		QueryResult<SuperRows<String,String,String,String>> queryResult=query.execute();
		log.warn("Got previous values ");
		String currentRowKey="";
		String currentSuperColumn="";
		for (SuperRow<String,String,String,String> oneSuperRow: queryResult.get() )
		{
			if (oneSuperRow.getSuperSlice().getSuperColumns().size()>0)
			{

				currentRowKey=oneSuperRow.getKey();
				Map<String,Map<String,String>> oneSuperColumn = new LinkedHashMap<String, Map<String,String>>();
				for (HSuperColumn<String,String,String> oneSuperColumnResult: oneSuperRow.getSuperSlice().getSuperColumns())
				{
					currentSuperColumn= oneSuperColumnResult.getName();
					Map<String,String> oneSuperColumnRowAttribute = new LinkedHashMap<String,String>();
					for (HColumn<String,String> oneColumn : oneSuperColumnResult.getColumns())
					{
						oneSuperColumnRowAttribute.put(oneColumn.getName(), oneColumn.getValue());
					}
					oneSuperColumn.put(currentSuperColumn, oneSuperColumnRowAttribute);
					
				}
				

				oneResult.put(currentRowKey,  oneSuperColumn);
	
			}
		}
				
				
	
		hTemplate=null;
		query=null;
		return oneResult;
	}

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

	public List<String> getSuperColumns() {
		return superColumns;
	}

	public void setSuperColumns(List<String> superColumns) {
		this.superColumns = superColumns;
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

	public MultiGetSuperSliceQuery() {
		super();
		// TODO Auto-generated constructor stub
	}

	public MultiGetSuperSliceQuery(ElasticCluster elasticCluster,
			String keyspaceName, String columnFamilyName, List<String> keys,
			List<String> superColumns, boolean reverseResultOrder) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.keys = keys;
		this.superColumns = superColumns;
		this.reverseResultOrder = reverseResultOrder;
	}

	public MultiGetSuperSliceQuery(ElasticCluster elasticCluster,
			String keyspaceName, String columnFamilyName, List<String> keys,
			String startSuperColumn, String endSuperColumn,
			boolean reverseResultOrder) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.keys = keys;
		this.startSuperColumn = startSuperColumn;
		this.endSuperColumn = endSuperColumn;
		this.reverseResultOrder = reverseResultOrder;
	}

}
