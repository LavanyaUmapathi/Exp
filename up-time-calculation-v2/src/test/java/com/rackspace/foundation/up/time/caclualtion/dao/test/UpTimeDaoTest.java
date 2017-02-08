package com.rackspace.foundation.up.time.caclualtion.dao.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.rackspace.foundation.up.time.calculation.dao.impl.UpTimeCalculationDaoImpl;

public class UpTimeDaoTest {
	private UpTimeCalculationDaoImpl upTimeCalc;
	Long startTimeStamp=1434949200L;//06-20-2015
	Long endTimeStamp=1435032000L; //06-23-2015
    @Before
    public void setUp(){
    	List<String> seedList = new ArrayList<String>();
    	seedList.add("10.13.230.52");
    	upTimeCalc = new UpTimeCalculationDaoImpl("Cassandra2",
    							seedList);
    }
    @After
    public void shutDown(){
    	
    	upTimeCalc.close();
    }
	@Test
	public void testNextStartTime() {
		System.out.println("Running testNextStartTime");
		String currentTime = String.valueOf(Calendar.getInstance().getTimeInMillis()/1000L);
		upTimeCalc.setNextStartTime(currentTime, "MaaS150");
		Assert.assertTrue(currentTime.compareTo(upTimeCalc.getNextStartTimeStamp("MaaS150"))==0);
	}
	@Test
	public void testNextEndTimeStamp(){
		System.out.println("Running testNextEndTime");
		String currentTime = String.valueOf(Calendar.getInstance().getTimeInMillis()/1000L);
		upTimeCalc.setNextUpTimeCalcTimeStamp(currentTime, "MaaS150");
		Assert.assertTrue(currentTime.compareTo(upTimeCalc.getEndTimeStamp("MaaS150"))==0);
	}
	@Test
	public void testPollStateErrors() throws Exception{
		System.out.println("Running testPollStateErrors");
		List<String> monitors=upTimeCalc.getLastEnabledMonitors(String.valueOf(this.startTimeStamp), "MaaS10");
		Assert.assertNotNull(monitors);
		Assert.assertTrue(monitors.size()>0);
		System.out.println("***********************");
		
		Map<String, Map<String, Map<String,String>>> pollStateErrorsRecords = upTimeCalc.getAllPollStateErrors(monitors, this.startTimeStamp,  this.endTimeStamp);

		System.out.println("**********************");
		Assert.assertNotNull(pollStateErrorsRecords);
		System.out.println("Poll State Error size is : "+pollStateErrorsRecords.size());
	}
	@Test 
	public void testSupressionIntervals() throws Exception{
		System.out.println("Running test supressions");
		List<String> monitors=upTimeCalc.getLastEnabledMonitors(String.valueOf(this.startTimeStamp), "MaaS10");
		Assert.assertNotNull(monitors);
		Assert.assertTrue(monitors.size()>0);
		Map<String, Map<String, Map<String,String>>> pollStateErrorsRecords = upTimeCalc.getAllPollStateErrors(monitors, this.startTimeStamp,  this.endTimeStamp);
		
		List<Object> errorMonitors= ImmutableList.copyOf(pollStateErrorsRecords.keySet().toArray());
		List<String> allDevices = new ArrayList<String>();
		for (Object oneKey: errorMonitors){
			StringTokenizer tokenizer = new StringTokenizer((String)oneKey, ":");
			List<String> keyTokens = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				keyTokens.add(tokenizer.nextToken());
			}

			tokenizer = null;

			
			allDevices.add(keyTokens.get(0) + ":" + keyTokens.get(1));

		}
		
		
		Map<String, Map<String, Map<String,String>>> supressionRecords = upTimeCalc.getAllSupression(allDevices, this.startTimeStamp, this.endTimeStamp);
		
		Assert.assertNotNull(supressionRecords);
		System.out.println("Supression size: "+supressionRecords.size());
		
		
	}
	
    @Test
    public void testCheckCalculation() throws Exception{
    	System.out.println("running check calulcation");
    	List<String> lastEnabledMonitors = upTimeCalc.getLastEnabledMonitors("MaaS10");
    	Assert.assertNotNull(lastEnabledMonitors);
    	Assert.assertTrue(lastEnabledMonitors.size()> 0);
    	
    	System.out.println("Enabled Mon OK");
    	
    	List<Map<String,List<Map<String,String>>>> pollStateErrors= new ArrayList<Map<String,List<Map<String,String>>>>();
    	List<String> enabledMonitorsWithErrors= new ArrayList<String>();
    	int i=0;
    	int numberOfErrorsFound = 0;
    	System.out.println("Calling for ERRORS");
    	for (String oneEnabledMonitor : lastEnabledMonitors){
    		List<Map<String,String>> oneMonitorErrors= new ArrayList<Map<String,String>>();
    		
    		oneMonitorErrors= upTimeCalc.getPollStateErrors(oneEnabledMonitor, startTimeStamp, endTimeStamp);
    		if (oneMonitorErrors.size()>0){
    			Map<String,List<Map<String,String>>> oneMonitorPollStateErrors= new  HashMap<String, List<Map<String,String>>>();
    			oneMonitorPollStateErrors.put(oneEnabledMonitor, oneMonitorErrors);
    		    pollStateErrors.add(oneMonitorPollStateErrors);
    		    enabledMonitorsWithErrors.add(oneEnabledMonitor);
    		    numberOfErrorsFound +=oneMonitorErrors.size();
    		}
    		if (pollStateErrors.size()> 5){
    			break;
    		}
    		i++;
    	}
    	Assert.assertTrue(pollStateErrors.size()> 0);
    	//Assert.assertTrue(i> 5);
    	System.out.println("Geting Calc");
    	Map<String, Map<String, Map<String, String>>> hourlyCalculationResult = upTimeCalc.getAllHourlyUpTimeCalc(enabledMonitorsWithErrors, startTimeStamp, endTimeStamp);
    	Assert.assertTrue(hourlyCalculationResult.size()> 0);
    	
    	int numberOfCalcFound = 0;
    	
    	for (Map<String,List<Map<String,String>>> oneMonitorWithErrors : pollStateErrors)
    	{
    		String oneMonitorKey= oneMonitorWithErrors.keySet().iterator().next();
    		for (Map<String,String> oneErrorPoll : oneMonitorWithErrors.get(oneMonitorKey)){
    			long  errorStartTime = Long.valueOf(oneErrorPoll.get("StartTimeStamp")).longValue();
    			
    			Map<String,Map<String,String>> oneMonitorUpTimeCaclualation = hourlyCalculationResult.get(oneMonitorKey);
    			
    			Iterator<String> upTimeCalcIterator = oneMonitorUpTimeCaclualation.keySet().iterator();
    			while (upTimeCalcIterator.hasNext()){
    				long calculationStartTime = Long.valueOf(upTimeCalcIterator.next()).longValue();
    				if(errorStartTime >=calculationStartTime && errorStartTime< calculationStartTime+3600L){
    					if (Integer.valueOf(oneMonitorUpTimeCaclualation.get(String.valueOf(calculationStartTime)).get("TotalDownTime")) == 0)
    					{
    						System.out.println("Problem: "+ oneMonitorKey+" "+ calculationStartTime);
    					}
    					
    					Assert.assertTrue(Integer.valueOf(oneMonitorUpTimeCaclualation.get(String.valueOf(calculationStartTime)).get("TotalDownTime")) > 0);
    					Assert.assertTrue(Double.valueOf(oneMonitorUpTimeCaclualation.get(String.valueOf(calculationStartTime)).get("TotalAvailabilityPercent")) < 1D);
    					numberOfCalcFound++;
    				}
    			}
    			
    		}
    		
    		
    	}
    	Assert.assertTrue(numberOfCalcFound==numberOfErrorsFound );
    	System.out.println("Done");
    	
    }
    @Test
    public void testCheckCalculationInBatch() throws Exception{
    	System.out.println("run check calc in batch");
    	List<String> lastEnabledMonitors = upTimeCalc.getLastEnabledMonitors("MaaS10");
    	Assert.assertNotNull(lastEnabledMonitors);
    	Assert.assertTrue(lastEnabledMonitors.size()> 0);
    	
    	
    	Map<String,Map<String,Map<String,String>>> pollStateErrors= new HashMap<String,Map<String,Map<String,String>>>();
    	HashSet<String> enabledMonitorsWithErrors= new HashSet<String>();
    	List<String> enabledMonitorsBatch = new ArrayList<String>();
    	int i=0;
    	
    	for (String oneEnabledMonitor : lastEnabledMonitors){
    		enabledMonitorsBatch.add(oneEnabledMonitor);
    		if (enabledMonitorsBatch.size()== 1000){
    			Map<String,Map<String,Map<String,String>>> batchErrors=upTimeCalc.getAllPollStateErrors(enabledMonitorsBatch, startTimeStamp, endTimeStamp);
    			Iterator<Entry<String,Map<String,Map<String,String>>>> batchErrorsIterator = batchErrors.entrySet().iterator();
    			while (batchErrorsIterator.hasNext()){
    				Entry <String,Map<String,Map<String,String>>> oneBatchError = batchErrorsIterator.next();
    				
       				enabledMonitorsWithErrors.add(oneBatchError.getKey());
    				pollStateErrors.put(oneBatchError.getKey(), oneBatchError.getValue());
    				
    				
    				
    			}
    			enabledMonitorsBatch = new ArrayList<String>();
    		}
    		if (enabledMonitorsWithErrors.size()>1000){
    			break;
    		}
    	}
    	Assert.assertTrue(pollStateErrors.size()> 0);
    	Map<String, Map<String, Map<String, String>>> hourlyCalculationResult = upTimeCalc.getAllHourlyUpTimeCalc(new ArrayList<String>(enabledMonitorsWithErrors), startTimeStamp, endTimeStamp);
    	Assert.assertTrue(hourlyCalculationResult.size()> 0);
    	
    	
    
		Iterator<Entry<String,Map<String,Map<String,String>>>> pollStateErrorsIterator = pollStateErrors.entrySet().iterator();
		
		
    	
    	while(pollStateErrorsIterator.hasNext())
    	{
    		Entry<String,Map<String,Map<String,String>>> onePollStateErrorEntry = pollStateErrorsIterator.next();
    		String oneMonitorKey= onePollStateErrorEntry.getKey();
    		Iterator <Entry<String,Map<String,String>>> errorIterator = onePollStateErrorEntry.getValue().entrySet().iterator();
    		while (errorIterator.hasNext()){
    			Entry<String,Map<String,String>> oneError = errorIterator.next();
    			long  errorStartTime = Long.valueOf(oneError.getKey());
    			long errorEndTime = Long.valueOf(oneError.getValue().get("EndTime"));
    			Map<String,Map<String,String>> oneMonitorUpTimeCaclualation = hourlyCalculationResult.get(oneMonitorKey);
    			if (oneMonitorUpTimeCaclualation.size()==0){
    				System.out.println(oneMonitorKey+" no calculation");
    			}
    			Iterator<String> upTimeCalcIterator = oneMonitorUpTimeCaclualation.keySet().iterator();
    			while (upTimeCalcIterator.hasNext()){
    				long calculationStartTime = Long.valueOf(upTimeCalcIterator.next()).longValue();
    				if((errorStartTime >=calculationStartTime && errorStartTime< calculationStartTime+3600L) || (errorStartTime <=calculationStartTime && errorEndTime==0) || (errorStartTime <=calculationStartTime && errorEndTime >=calculationStartTime)  ){
    					
    					Assert.assertTrue(Integer.valueOf(oneMonitorUpTimeCaclualation.get(String.valueOf(calculationStartTime)).get("TotalDownTime")) > 0);
    					Assert.assertTrue(Double.valueOf(oneMonitorUpTimeCaclualation.get(String.valueOf(calculationStartTime)).get("TotalAvailabilityPercent")) < 1D);
    					
    				}
    				
    				
    			}
    		}
    	}
    				
    	System.out.println(" end run check calc in batch");
    	
    	
    }
    @Test
    public void checkCalcResult() throws Exception{
    	System.out.println("run check calc result");
    	List<String> lastEnabledMonitors = upTimeCalc.getLastEnabledMonitors("MaaS5");
    	Assert.assertNotNull(lastEnabledMonitors);
    	Assert.assertTrue(lastEnabledMonitors.size()> 0);
    	
    	int recordsChecked = 0;
    	List<String> enabledMonitorBatch = new ArrayList<String>();
    	enoughChecked:
    	for (String oneEnabledMonitor : lastEnabledMonitors){
    		enabledMonitorBatch.add(oneEnabledMonitor);
    		if (enabledMonitorBatch.size()==1000){
    			Map<String, Map<String, Map<String, String>>> hourlyCalculationResult = upTimeCalc.getAllHourlyUpTimeCalc(enabledMonitorBatch, startTimeStamp, endTimeStamp);
    			Iterator<Entry<String, Map<String, Map<String, String>>>> hourlyCalculationResultIterator = hourlyCalculationResult.entrySet().iterator();
    			while (hourlyCalculationResultIterator.hasNext()){
    				Entry<String, Map<String, Map<String, String>>> oneEntry= hourlyCalculationResultIterator.next();
    				String oneKey = oneEntry.getKey();
    				Map<String, Map<String, String>> oneKeyCalc = oneEntry.getValue();
    				Iterator<Entry<String,  Map<String, String>>> oneCalcIterator = oneKeyCalc.entrySet().iterator();
    				while (oneCalcIterator.hasNext()){
    					Entry<String,  Map<String, String>> oneHourCalc= oneCalcIterator.next();
    					long hourDownTime= Long.valueOf(oneHourCalc.getValue().get("TotalDownTime"));
    					if (hourDownTime>0L){
    						long startTime = Long.valueOf(oneHourCalc.getKey());
    						List<String> keyList = new ArrayList<String>();
    						keyList.add(oneKey);
    		    			Map<String,Map<String,Map<String,String>>> errors=upTimeCalc.getAllPollStateErrors(keyList, startTime, startTime+3600L);
    		    			Assert.assertNotNull(errors);
    		    			Assert.assertTrue(errors.size()>0);
    						recordsChecked++;
    						if (recordsChecked> 100){
    							break enoughChecked;
    						}
    					}
    					
    				}
    			}
    			break;
    		}
    		
    	}
    	System.out.println(recordsChecked);
    	Assert.assertTrue(recordsChecked>0);
    	System.out.println("run check calc result");
    	
    }
}
