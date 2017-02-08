package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.apache.log4j.Logger;

import com.rackspace.foundation.cassandra.dao.query.impl.RangeSliceQueryResult;
import com.rackspace.foundation.cassandra.service.ElasticCluster;

public class RangeSliceQuery {
	private ElasticCluster elasticCluster;
	private String keyspaceName;
	private String columnFamilyName;
	private String startKey;
	private String endKey;
	
	private String startColumn;
	private String endColumn;
	private boolean reverseResultOrder;
	
	private int rowCount=1000;
	
	private static org.apache.log4j.Logger log = Logger
			.getLogger(RangeSliceQuery.class);
	
	public List<String> getKeysOnly(){
		List<String> allKeys = new ArrayList<String>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		RangeSlicesQuery<String, String,String> query=hTemplate.createRangeSlicesQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		
		System.out.println(this.keyspaceName);
		System.out.println(this.columnFamilyName);
		query.setColumnFamily(columnFamilyName);
		query.setKeys( "","");
		query.setReturnKeysOnly();
		
		
		query.setRowCount(1000);
		
		
		boolean moreKeys = true;
		int i=0;
		
		while (moreKeys)
		{
			log.info("Running query for :"+i);
			
			
			
			List<String> oneQueryResult =this.executeQuery(query);
			log.info("Executed query for :"+i);
			i++;
			if (oneQueryResult.size()>1)
			{
				if (allKeys.size()==0)
				{
				allKeys.addAll(oneQueryResult);
				}
				else
				{
					int lastIndex = allKeys.size()-1;
					allKeys.remove(lastIndex);
					allKeys.addAll(oneQueryResult);
				}
				
				query.setKeys(oneQueryResult.get(oneQueryResult.size()-1), "");
				
				
			}
			else
			{
				int lastIndex = allKeys.size()-1;
				allKeys.remove(lastIndex);
				allKeys.addAll(oneQueryResult);
				moreKeys=false;
			}
		}
		
		return allKeys;
		
	}
	
	private List<String> executeQuery(RangeSlicesQuery<String, String,String> query)
	{
		List<String> allQueryKeys = new ArrayList<String>();
		
		QueryResult<OrderedRows<String, String,String>> result= query.execute();
		Iterator<Row<String, String,String>> resultIterator = result.get().iterator();
		while (resultIterator.hasNext())
		{
			Row<String,String,String> oneRow = resultIterator.next();
			
			allQueryKeys.add(oneRow.getKey());
		}
		
		return allQueryKeys;
		
	}
	public RangeSliceQueryResult executeQuery()
	{
		RangeSliceQueryResult result = new RangeSliceQueryResult();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		
		RangeSlicesQuery<String, String,String> query=hTemplate.createRangeSlicesQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		
		query.setColumnFamily(this.columnFamilyName);
		query.setKeys(this.startKey, this.endKey);
		query.setRange(this.startColumn,this.endColumn, this.reverseResultOrder, 300000);
		QueryResult<OrderedRows<String,String,String>> queryResult =query.execute();
		OrderedRows<String,String,String> queryResultRows = queryResult.get();
		String currentKey="";
		Map<String,String> oneRowColumns = new HashMap<String,String>();
		Map<String, Map<String,String>> oneRowResult = new HashMap<String,Map<String, String>>();
		for (Row<String,String,String> oneRow : queryResultRows.getList())
		{
			if (currentKey.compareTo(oneRow.getKey())!=0)
			{
				if (currentKey.length()>0)
				{
					oneRowResult.clear();
					oneRowResult.put(currentKey, oneRowColumns);
					result.setRangeSliceQueryResult(oneRowResult);
					oneRowColumns.clear();
					
				}
				
				currentKey= oneRow.getKey();
				
			}
			for (HColumn<String, String> oneColumn: oneRow.getColumnSlice().getColumns())
			{
				oneRowColumns.put(oneColumn.getName(), oneColumn.getValue());
			}
		}
		return result;
	}

	public RangeSliceQuery() {
		super();
		// TODO Auto-generated constructor stub
	}

	public RangeSliceQuery(ElasticCluster elasticCluster, String keyspaceName,
			String columnFamilyName) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
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

	public String getStartKeys() {
		return this.startKey;
	}

	public void setStartKey(String startKeys) {
		this.startKey = startKeys;
	}
	
	public String getEndKeys() {
		return this.endKey;
	}

	public void setEndKey(String endKeys) {
		this.endKey = endKeys;
	}

	

	public String getStartColumn() {
		return startColumn;
	}

	public void setStartColumn(String startColumn) {
		this.startColumn = startColumn;
	}

	public String getEndColumn() {
		return endColumn;
	}

	public void setEndColumn(String endColumn) {
		this.endColumn = endColumn;
	}

	public boolean isReverseResultOrder() {
		return reverseResultOrder;
	}

	public void setReverseResultOrder(boolean reverseResultOrder) {
		this.reverseResultOrder = reverseResultOrder;
	}

	

	public RangeSliceQuery(ElasticCluster elasticCluster, String keyspaceName,
			String columnFamilyName, String startKey, String endKey, String startColumn,
			String endColumn, boolean reverseResultOrder) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.startKey=startKey;
		this.endKey=endKey;
		this.startColumn = startColumn;
		this.endColumn = endColumn;
		this.reverseResultOrder = reverseResultOrder;
	}

}
