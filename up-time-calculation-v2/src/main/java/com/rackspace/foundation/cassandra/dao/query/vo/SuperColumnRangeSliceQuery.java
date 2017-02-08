package com.rackspace.foundation.cassandra.dao.query.vo;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnRangeSliceQueryResult;
import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnRow;
import com.rackspace.foundation.cassandra.dao.query.vo.SuperColumnWithSuperColumnRow;
import com.rackspace.foundation.cassandra.service.ElasticCluster;

public class SuperColumnRangeSliceQuery {
	private static org.apache.log4j.Logger log = Logger
			.getLogger(SuperColumnRangeSliceQuery.class);
	
	private ElasticCluster elasticCluster;
	private String keyspaceName;
	private String columnFamilyName;
	private String oneKey;
	private List<String> superColumns;
	private String startSuperColumn;
	private String endSuperColumn;
	private boolean reverseResultOrder;
	
	public SuperColumnRangeSliceQueryResult executeQuery()
	{
		SuperColumnRangeSliceQueryResult result= new SuperColumnRangeSliceQueryResult();
		Map<String, SuperColumnWithSuperColumnRow> oneResult = new LinkedHashMap<String, SuperColumnWithSuperColumnRow>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		SuperSliceQuery<String,String, String,String> query=hTemplate.createSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		
		
		query.setColumnFamily( this.columnFamilyName);
		
		
		if (superColumns != null)
		{
			
			query.setColumnNames((String[])superColumns.toArray());
		}
		else
		{
			query.setRange(this.startSuperColumn, this.endSuperColumn, this.reverseResultOrder, 100000);
		}
		
	
		
		
			SuperColumnWithSuperColumnRow superColumnWithSuperColumnRow = new SuperColumnWithSuperColumnRow();
			
			
			
			query.setKey(oneKey);
			
			QueryResult<SuperSlice<String,String,String>>queryResult = query.execute();
			SuperSlice<String,String,String> resultRows=queryResult.get();
			
			List<HSuperColumn<String,String,String>> superColumnList = resultRows.getSuperColumns();
			
			Iterator<HSuperColumn<String,String,String>> superColumnListIterator= superColumnList.iterator();
			
			Map <String, SuperColumnRow> superColumnWithRowMap= new LinkedHashMap<String, SuperColumnRow>();
			while (superColumnListIterator.hasNext())
			{
				HSuperColumn<String,String,String> oneSuperColumn = superColumnListIterator.next();
				
				List<HColumn<String,String>> rowColumns = oneSuperColumn.getColumns();
				Iterator<HColumn<String,String>> oneRowColumnIterator= rowColumns.iterator();
				SuperColumnRow superColumnRow = new SuperColumnRow();
				Map<String, String>  oneRow = new LinkedHashMap<String, String>();
				while (oneRowColumnIterator.hasNext())
				{
					HColumn<String,String> oneRowColumn=oneRowColumnIterator.next();
					oneRow.put(oneRowColumn.getName(), oneRowColumn.getValue());
					superColumnRow.setRowColumns(oneRow);
					oneRowColumn=null;
					
				}
				
				superColumnWithRowMap.put(oneSuperColumn.getName(),superColumnRow);
				
				
				
			
				
				oneSuperColumn=null;
				rowColumns=null;
				oneRowColumnIterator=null;
			}
			
			if (superColumnWithRowMap.size()>0)
			{
			superColumnWithSuperColumnRow.setSuperColumnWithSuperColumnRow(superColumnWithRowMap);
			
			oneResult.put(oneKey, superColumnWithSuperColumnRow);
			}
			
			
			
			queryResult=null;
			superColumnList=null;
			superColumnListIterator=null;
		
		result.setQueryResult(oneResult);
		oneResult=null;
		hTemplate=null;
		query=null;
		return result;
	}
	public SuperColumnRangeSliceQueryResult getPreviousSupercolumn()
	{
		log.info("In  superColumnRangeSliceQuery query for: ");
		log.info(this.columnFamilyName);
		log.info(this.oneKey);
		log.info(this.reverseResultOrder);
		log.info(this.startSuperColumn);
		
		SuperColumnRangeSliceQueryResult result= new SuperColumnRangeSliceQueryResult();
		Map<String, SuperColumnWithSuperColumnRow> oneResult = new LinkedHashMap<String, SuperColumnWithSuperColumnRow>();
		HectorTemplate hTemplate=elasticCluster.getHectorTemplate(this.keyspaceName);
		SuperSliceQuery<String,String, String,String> query=hTemplate.createSuperSliceQuery(StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		query.setColumnFamily( this.columnFamilyName);
		
		
		
		
			query.setRange(  this.startSuperColumn,this.endSuperColumn,true, 1);
		
		
	
		
		
			SuperColumnWithSuperColumnRow superColumnWithSuperColumnRow = new SuperColumnWithSuperColumnRow();
			
			
			log.info("Set key");
			
			query.setKey(oneKey);
			log.info("Key set");
			log.info("Starting query");
			QueryResult<SuperSlice<String,String,String>>queryResult = query.execute();
			log.info("Finished query");
			SuperSlice<String,String,String> resultRows=queryResult.get();
			
			List<HSuperColumn<String,String,String>> superColumnList = resultRows.getSuperColumns();
			log.info("got result:"+superColumnList.size());
			
			Iterator<HSuperColumn<String,String,String>> superColumnListIterator= superColumnList.iterator();
			
			Map <String, SuperColumnRow> superColumnWithRowMap= new LinkedHashMap<String, SuperColumnRow>();
			while (superColumnListIterator.hasNext())
			{
				HSuperColumn<String,String,String> oneSuperColumn = superColumnListIterator.next();
				
				List<HColumn<String,String>> rowColumns = oneSuperColumn.getColumns();
				Iterator<HColumn<String,String>> oneRowColumnIterator= rowColumns.iterator();
				SuperColumnRow superColumnRow = new SuperColumnRow();
				Map<String, String>  oneRow = new LinkedHashMap<String, String>();
				while (oneRowColumnIterator.hasNext())
				{
					HColumn<String,String> oneRowColumn=oneRowColumnIterator.next();
					oneRow.put(oneRowColumn.getName(), oneRowColumn.getValue());
					superColumnRow.setRowColumns(oneRow);
					oneRowColumn=null;
					
				}
				
				superColumnWithRowMap.put(oneSuperColumn.getName(),superColumnRow);
				
				
				
			
				
				oneSuperColumn=null;
				rowColumns=null;
				oneRowColumnIterator=null;
			}
			if (superColumnWithRowMap.size()>0)
			{
			superColumnWithSuperColumnRow.setSuperColumnWithSuperColumnRow(superColumnWithRowMap);
			
			oneResult.put(oneKey, superColumnWithSuperColumnRow);
			}
			
			
			
			queryResult=null;
			superColumnList=null;
			superColumnListIterator=null;
		
		result.setQueryResult(oneResult);
		oneResult=null;
		hTemplate=null;
		query=null;
		log.info("return result");
		return result;
	}

	public SuperColumnRangeSliceQuery() {
		super();
		// TODO Auto-generated constructor stub
	}

	public SuperColumnRangeSliceQuery(ElasticCluster elasticCluster,
			String keyspaceName, String columnFamilyName, String oneKey,
			String startSuperColumn, String endSuperColumn,
			boolean reverseResultOrder) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.oneKey = oneKey;
		this.startSuperColumn = startSuperColumn;
		this.endSuperColumn = endSuperColumn;
		this.reverseResultOrder = reverseResultOrder;
	}

	public SuperColumnRangeSliceQuery(ElasticCluster elasticCluster,
			String keyspaceName, String columnFamilyName,String oneKey,
			List<String> superColumns, boolean reverseResultOrder) {
		super();
		this.elasticCluster = elasticCluster;
		this.keyspaceName = keyspaceName;
		this.columnFamilyName = columnFamilyName;
		this.oneKey = oneKey;
		this.superColumns = superColumns;
		this.reverseResultOrder = reverseResultOrder;
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
	public String getOneKey() {
		return oneKey;
	}
	public void setOneKey(String oneKey) {
		this.oneKey = oneKey;
	}

}
